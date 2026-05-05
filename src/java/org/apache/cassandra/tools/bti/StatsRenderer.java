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
package org.apache.cassandra.tools.bti;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Visitor that accumulates statistics over the full trie traversal and prints a summary on {@link #end}.
 * Always completes a full traversal (maxNodes should be 0 when this renderer is used).
 */
public class StatsRenderer implements TrieViewVisitor
{
    private final PrintStream out;
    private final String fileName;
    private final long fileSize;

    private long keyCount;
    private String firstName;
    private String lastName;

    private long totalNodes = 0;
    private final Map<String, Long> nodeTypeCounts = new LinkedHashMap<>();
    private long payloadedNodes = 0;
    private long crossPageEdges = 0;
    private long dataFilePointers = 0;
    private long rowIndexPointers = 0;
    private int maxDepth = 0;
    private final long[] depthCounts = new long[1024];
    private long totalEdges = 0;

    public StatsRenderer(String fileName, long fileSize, PrintStream out)
    {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.out = out;
    }

    @Override
    public void begin(long keyCount, String firstName, String lastName)
    {
        this.keyCount = keyCount;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    @Override
    public void visitNode(PartitionIndexWalker.NodeInfo node)
    {
        totalNodes++;
        nodeTypeCounts.merge(node.nodeTypeName, 1L, Long::sum);
        if (node.depth > maxDepth) maxDepth = node.depth;
        if (node.depth < depthCounts.length) depthCounts[node.depth]++;
        if (node.crossesPage) crossPageEdges++;
        if (node.depth > 0) totalEdges++;
        if (node.hasPayload)
        {
            payloadedNodes++;
            if (node.isDataFile) dataFilePointers++;
            else rowIndexPointers++;
        }
    }

    @Override
    public void end(boolean truncated)
    {
        out.println("BTI Partition Index");
        out.printf("  File        : %s%n", fileName);
        if (fileSize > 0) out.printf("  File size   : %,d bytes%n", fileSize);
        out.printf("  Keys indexed: %,d%n", keyCount);
        if (firstName != null) out.printf("  First key   : %s%n", firstName);
        if (lastName  != null) out.printf("  Last key    : %s%n", lastName);
        out.println();

        out.println("Node Type Distribution:");
        nodeTypeCounts.entrySet().stream()
                      .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                      .forEach(e -> {
                          double pct = totalNodes == 0 ? 0 : 100.0 * e.getValue() / totalNodes;
                          out.printf("  %-24s: %,8d  (%5.1f%%)%n", e.getKey(), e.getValue(), pct);
                      });
        out.printf("  %-24s: %,8d%n", "Total nodes", totalNodes);
        if (truncated) out.println("  (WARNING: traversal truncated — stats are partial)");
        out.println();

        out.println("Depth Distribution:");
        long modeCount = 0;
        for (int d = 0; d <= maxDepth && d < depthCounts.length; d++)
            if (depthCounts[d] > modeCount) modeCount = depthCounts[d];
        for (int d = 0; d <= maxDepth && d < depthCounts.length; d++)
        {
            if (depthCounts[d] == 0) continue;
            boolean isMode = depthCounts[d] == modeCount;
            out.printf("  Depth %3d : %,8d nodes%s%n", d, depthCounts[d], isMode ? "  ← mode" : "");
        }
        out.printf("  Max depth : %d%n", maxDepth);
        out.println();

        out.println("Space Efficiency:");
        if (keyCount > 0 && fileSize > 0)
            out.printf("  Bytes / key        : %.1f%n", (double) fileSize / keyCount);
        if (totalEdges > 0)
        {
            out.printf("  Cross-page edges   : %,d  (%.1f%%)%n",
                       crossPageEdges, 100.0 * crossPageEdges / totalEdges);
            out.printf("  Same-page edges    : %,d  (%.1f%%)%n",
                       totalEdges - crossPageEdges, 100.0 * (totalEdges - crossPageEdges) / totalEdges);
        }
        out.println();

        if (payloadedNodes > 0)
        {
            out.println("Payload Distribution:");
            out.printf("  Direct → data file : %,d  (%.1f%%)%n",
                       dataFilePointers, 100.0 * dataFilePointers / payloadedNodes);
            out.printf("  Via row index      : %,d  (%.1f%%)%n",
                       rowIndexPointers, 100.0 * rowIndexPointers / payloadedNodes);
        }
    }
}
