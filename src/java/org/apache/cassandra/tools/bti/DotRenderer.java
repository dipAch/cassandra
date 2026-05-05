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

/**
 * Visitor that emits a Graphviz DOT file for the partition index trie.
 *
 * <p>Render with:
 * <pre>
 *   dot -Tsvg trie.dot > trie.svg &amp;&amp; open trie.svg
 *   dot -Tpng trie.dot > trie.png
 * </pre>
 *
 * <p>Node color families:
 * <ul>
 *   <li>Light yellow  — PAYLOAD_ONLY (leaf / key terminus)
 *   <li>Light green   — SINGLE* (single-child chain)
 *   <li>Light blue    — SPARSE* (small branch)
 *   <li>Light orange  — DENSE* / LONG_DENSE (wide branch)
 * </ul>
 *
 * <p>Edge style:
 * <ul>
 *   <li>Dashed — parent and child share the same 4 KiB page (no cache miss)
 *   <li>Solid  — edge crosses a page boundary (potential cache miss)
 * </ul>
 */
public class DotRenderer implements TrieViewVisitor
{
    private final PrintStream out;

    public DotRenderer(PrintStream out)
    {
        this.out = out;
    }

    @Override
    public void begin(long keyCount, String firstName, String lastName)
    {
        out.println("digraph bti_partition_index {");
        out.printf("  // BTI partition index — %,d keys%n", keyCount);
        if (firstName != null) out.printf("  // First key: %s%n", firstName);
        if (lastName  != null) out.printf("  // Last key:  %s%n", lastName);
        out.println("  graph [rankdir=TB fontname=monospace];");
        out.println("  node [shape=box fontname=monospace fontsize=10];");
        out.println("  edge [fontname=monospace fontsize=9];");
        out.println();
    }

    @Override
    public void visitNode(PartitionIndexWalker.NodeInfo node)
    {
        String id = nodeId(node.position);
        String label = buildLabel(node);
        String fillColor = nodeColor(node.nodeTypeName);
        out.printf("  %s [label=\"%s\" fillcolor=\"%s\" style=filled];%n", id, label, fillColor);

        if (node.depth > 0)
        {
            String parentId = nodeId(node.parentPosition);
            String edgeLabel = String.format("%02x", node.path[node.depth - 1] & 0xFF);
            String style = node.crossesPage ? "solid" : "dashed";
            String penwidth = node.crossesPage ? " penwidth=2" : "";
            out.printf("  %s -> %s [label=\"%s\" style=%s%s];%n",
                       parentId, id, edgeLabel, style, penwidth);
        }
    }

    private String buildLabel(PartitionIndexWalker.NodeInfo node)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(node.nodeTypeName);
        sb.append("\\n@0x").append(Long.toHexString(node.position));
        if (node.childCount > 0)
            sb.append("\\n").append(node.childCount).append(" children");
        if (node.hasPayload)
        {
            if (node.isDataFile)
                sb.append("\\ndfile:0x").append(Long.toHexString(~node.payloadIndexPos));
            else
                sb.append("\\nifile:0x").append(Long.toHexString(node.payloadIndexPos));
            if (node.decodedKey != null)
                sb.append("\\n").append(escapeDot(node.decodedKey));
        }
        return sb.toString();
    }

    private static String nodeId(long pos)
    {
        // DOT node IDs must be valid identifiers; prefix with 'n' and use hex.
        return "n" + Long.toHexString(pos & Long.MAX_VALUE);
    }

    private static String nodeColor(String typeName)
    {
        if (typeName.startsWith("Payload")) return "#f9e79f";  // light yellow
        if (typeName.startsWith("Single"))  return "#a9dfbf";  // light green
        if (typeName.startsWith("Sparse"))  return "#aed6f1";  // light blue
        return "#f0b27a";                                       // light orange (Dense, LongDense)
    }

    private static String escapeDot(String s)
    {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    @Override
    public void end(boolean truncated)
    {
        if (truncated)
            out.println("  // WARNING: traversal truncated by --max-nodes or --max-depth limit");
        out.println("}");
    }
}
