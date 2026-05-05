/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.format.bti.PartitionIndex;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.tools.bti.AsciiTreeRenderer;
import org.apache.cassandra.tools.bti.DotRenderer;
import org.apache.cassandra.tools.bti.PartitionIndexWalker;
import org.apache.cassandra.tools.bti.StatsRenderer;
import org.apache.cassandra.tools.bti.TrieViewVisitor;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Standalone tool for inspecting the BTI-format partition index trie of an SSTable.
 *
 * <p>Usage examples:
 * <pre>
 *   # Summary stats (works at any scale)
 *   bin/sstabletrieview data/data/ks/tbl/nb-1-bti-Partitions.db
 *
 *   # ASCII tree (good for 5–200 keys)
 *   bin/sstabletrieview --mode ascii --decode-keys nb-1-bti-Partitions.db
 *
 *   # Graphviz DOT (render with: dot -Tsvg trie.dot > trie.svg)
 *   bin/sstabletrieview --mode dot --out trie.dot nb-1-bti-Data.db
 *
 *   # Raw Walker.dumpTrie passthrough (compare with ascii output)
 *   bin/sstabletrieview --mode raw nb-1-bti-Partitions.db
 * </pre>
 *
 * <p>The tool accepts any BTI SSTable component file (Data.db, Partitions.db, etc.) —
 * it derives the Partitions.db path from the SSTable descriptor embedded in the filename.
 *
 * <p>Requires BTI-format SSTables. To enable BTI:
 * <pre>
 *   # In cassandra.yaml:
 *   sstable_format: bti
 *
 *   # Or per table:
 *   ALTER TABLE ks.tbl WITH sstable_format = 'bti';
 * </pre>
 */
@SuppressWarnings("deprecation")  // PosixParser is deprecated but used for consistency with other tools
public class SSTableTrieView
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static final String MODE_OPT       = "mode";
    private static final String MAX_NODES_OPT  = "max-nodes";
    private static final String MAX_DEPTH_OPT  = "max-depth";
    private static final String DECODE_KEYS_OPT = "decode-keys";
    private static final String OUT_OPT        = "out";
    private static final String NO_COLOR_OPT   = "no-color";
    private static final String HELP_OPT       = "h";

    private static final Options OPTIONS = new Options();

    static
    {
        OPTIONS.addOption(Option.builder().longOpt(MODE_OPT)
                                .hasArg().argName("mode")
                                .desc("Output mode: stats (default), ascii, dot, raw")
                                .build());
        OPTIONS.addOption(Option.builder().longOpt(MAX_NODES_OPT)
                                .hasArg().argName("n")
                                .desc("Max nodes to visit in ascii/dot modes (default: 2000, 0 = unlimited)")
                                .build());
        OPTIONS.addOption(Option.builder().longOpt(MAX_DEPTH_OPT)
                                .hasArg().argName("n")
                                .desc("Max trie depth to descend (default: unlimited)")
                                .build());
        OPTIONS.addOption(Option.builder().longOpt(DECODE_KEYS_OPT)
                                .desc("Attempt to decode partition keys from trie path bytes (approximate)")
                                .build());
        OPTIONS.addOption(Option.builder().longOpt(OUT_OPT)
                                .hasArg().argName("file")
                                .desc("Write output to file instead of stdout")
                                .build());
        OPTIONS.addOption(Option.builder().longOpt(NO_COLOR_OPT)
                                .desc("Disable ANSI color in ascii mode")
                                .build());
        OPTIONS.addOption(Option.builder(HELP_OPT).longOpt("help")
                                .desc("Show this help message")
                                .build());
    }

    public static void main(String[] args) throws Exception
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try
        {
            cmd = parser.parse(OPTIONS, args);
        }
        catch (ParseException e)
        {
            System.err.println(e.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        if (cmd.hasOption(HELP_OPT) || cmd.getArgs().length == 0)
        {
            printUsage();
            System.exit(cmd.hasOption(HELP_OPT) ? 0 : 1);
            return;
        }

        String mode = cmd.getOptionValue(MODE_OPT, "stats");
        // Stats mode always traverses everything; visual modes default to 2000 nodes.
        boolean isVisualMode = "ascii".equals(mode) || "dot".equals(mode);
        int maxNodes = Integer.parseInt(cmd.getOptionValue(MAX_NODES_OPT, isVisualMode ? "2000" : "0"));
        int maxDepth = Integer.parseInt(cmd.getOptionValue(MAX_DEPTH_OPT, "0"));
        boolean decodeKeys = cmd.hasOption(DECODE_KEYS_OPT);
        boolean color = !cmd.hasOption(NO_COLOR_OPT) && System.console() != null;
        String outFile = cmd.getOptionValue(OUT_OPT);

        File ssTableFile = new File(cmd.getArgs()[0]);
        if (!ssTableFile.exists())
        {
            System.err.printf("File not found: %s%n", ssTableFile);
            System.exit(1);
            return;
        }

        Descriptor desc;
        try
        {
            desc = Descriptor.fromFileWithComponent(ssTableFile, false).left;
        }
        catch (Exception e)
        {
            System.err.printf("Cannot parse SSTable descriptor from '%s': %s%n", ssTableFile, e.getMessage());
            System.exit(1);
            return;
        }

        if (!desc.getFormat().name().equals(BtiFormat.NAME))
        {
            System.err.printf("Error: '%s' is in '%s' format, not BTI format.%n",
                              ssTableFile.name(), desc.getFormat().name());
            System.err.println("This tool only supports BTI-format SSTables (uses partition index trie).");
            System.err.println("To enable BTI format, add to cassandra.yaml:");
            System.err.println("  sstable_format: bti");
            System.err.println("Or per table: ALTER TABLE ks.tbl WITH sstable_format = 'bti';");
            System.exit(1);
            return;
        }

        File partIndexFile = desc.fileFor(BtiFormat.Components.PARTITION_INDEX);
        if (!partIndexFile.exists())
        {
            System.err.printf("Partition index file not found: %s%n", partIndexFile);
            System.exit(1);
            return;
        }

        IPartitioner partitioner;
        try
        {
            partitioner = FBUtilities.newPartitioner(desc);
        }
        catch (Exception e)
        {
            System.err.printf("Cannot determine partitioner: %s%n", e.getMessage());
            System.exit(1);
            return;
        }

        if ("raw".equals(mode))
        {
            runRawMode(partIndexFile, partitioner, outFile);
            return;
        }

        try (PartitionIndex index = PartitionIndex.load(new FileHandle.Builder(partIndexFile), partitioner, false))
        {
            String firstName = formatKey(index.firstKey());
            String lastName  = formatKey(index.lastKey());
            long fileSize    = partIndexFile.length();

            PrintStream out = openOutputStream(outFile);
            try (TrieViewVisitor visitor = createVisitor(mode, partIndexFile.name(), fileSize, color, out))
            {
                visitor.begin(index.size(), firstName, lastName);
                try (PartitionIndexWalker walker = new PartitionIndexWalker(
                        index, maxNodes, maxDepth, decodeKeys ? partitioner : null, decodeKeys))
                {
                    walker.walk(visitor);
                    visitor.end(walker.isTruncated());
                }
            }
            finally
            {
                if (outFile != null) out.close();
            }
        }
    }

    private static void runRawMode(File partIndexFile, IPartitioner partitioner, String outFile) throws IOException
    {
        try (PartitionIndex index = PartitionIndex.load(new FileHandle.Builder(partIndexFile), partitioner, false);
             PartitionIndex.Reader reader = index.openReader())
        {
            PrintStream out = openOutputStream(outFile);
            reader.dumpTrie(out,
                            (buf, ppos, pbits, version) -> Long.toString(reader.getSpecificIndexPos(ppos, pbits)),
                            null);
            if (outFile != null) out.close();
        }
    }

    private static TrieViewVisitor createVisitor(String mode, String fileName, long fileSize,
                                                  boolean color, PrintStream out)
    {
        switch (mode)
        {
            case "ascii": return new AsciiTreeRenderer(out, color);
            case "dot":   return new DotRenderer(out);
            default:      return new StatsRenderer(fileName, fileSize, out);
        }
    }

    private static String formatKey(DecoratedKey key)
    {
        if (key == null) return null;
        byte[] bytes = new byte[key.getKey().remaining()];
        key.getKey().duplicate().get(bytes);
        // Show as UTF-8 if all printable ASCII, otherwise hex.
        boolean printable = bytes.length > 0;
        for (byte b : bytes)
            if (b < 0x20 || b > 0x7e) { printable = false; break; }
        return printable ? new String(bytes, StandardCharsets.UTF_8)
                         : "0x" + PartitionIndexWalker.bytesToHex(bytes);
    }

    private static PrintStream openOutputStream(String outFile) throws IOException
    {
        return outFile == null ? System.out : new PrintStream(outFile, StandardCharsets.UTF_8.name());
    }

    private static void printUsage()
    {
        String header = "\nInspects the partition index trie of a BTI-format SSTable.\n\n";
        String footer = "\nExamples:\n"
                        + "  sstabletrieview nb-1-bti-Partitions.db\n"
                        + "  sstabletrieview --mode ascii --decode-keys nb-1-bti-Data.db\n"
                        + "  sstabletrieview --mode dot --max-nodes 200 --out trie.dot nb-1-bti-Data.db\n"
                        + "  dot -Tsvg trie.dot > trie.svg && open trie.svg\n"
                        + "  sstabletrieview --mode raw nb-1-bti-Partitions.db\n";
        new HelpFormatter().printHelp("sstabletrieview [options] <sstable-file>", header, OPTIONS, footer);
    }
}
