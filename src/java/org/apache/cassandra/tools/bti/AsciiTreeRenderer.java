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
 * Visitor that renders the partition index trie as an indented ASCII tree with box-drawing characters.
 *
 * <p>Color coding (when ANSI is enabled):
 * <ul>
 *   <li>Yellow  — PAYLOAD_ONLY (leaf nodes / key termini)
 *   <li>Green   — SINGLE / SINGLE_NOPAYLOAD (single-child chains)
 *   <li>Blue    — SPARSE (small branch nodes, 2–6 children)
 *   <li>Red     — DENSE / LONG_DENSE (wide branch nodes, contiguous transition range)
 * </ul>
 *
 * <p>Edge annotations:
 * <ul>
 *   <li>{@code †} before the transition byte hex means this edge crosses a 4 KiB page boundary
 *       (potential cache miss when the index is not resident in memory).
 *   <li>{@code dfile} = payload points directly into the data file (small partition).
 *   <li>{@code ifile} = payload points into the row index file (wide partition).
 * </ul>
 */
public class AsciiTreeRenderer implements TrieViewVisitor
{
    private static final String RESET  = "[0m";
    private static final String YELLOW = "[33m";  // PAYLOAD_ONLY
    private static final String GREEN  = "[32m";  // SINGLE*
    private static final String BLUE   = "[34m";  // SPARSE*
    private static final String RED    = "[31m";  // DENSE*

    private final PrintStream out;
    private final boolean color;

    /**
     * Tracks whether the node at each depth level was the last child of its parent.
     * Used to decide whether to draw "│   " (not last) or "    " (last) for ancestor levels.
     * Updated as each node is visited; because DFS is depth-first, entries at lower depths
     * reflect the current path from root to the node being visited.
     */
    private final boolean[] lastAtDepth = new boolean[1024];

    public AsciiTreeRenderer(PrintStream out, boolean color)
    {
        this.out = out;
        this.color = color;
    }

    @Override
    public void begin(long keyCount, String firstName, String lastName)
    {
        out.printf("BTI Partition Index  —  keys: %,d%n%n", keyCount);
    }

    @Override
    public void visitNode(PartitionIndexWalker.NodeInfo node)
    {
        if (node.depth == 0)
        {
            out.print("ROOT ");
            out.println(nodeLabel(node));
            return;
        }

        // Record whether this depth level is the last child (used by deeper nodes for prefix).
        lastAtDepth[node.depth] = node.isLastChild;

        // Build the prefix string: for each ancestor depth 1..depth-1 show continuation lines.
        StringBuilder prefix = new StringBuilder();
        for (int d = 1; d < node.depth; d++)
            prefix.append(lastAtDepth[d] ? "    " : "│   ");

        // Connector and edge annotation.
        String connector = node.isLastChild ? "└─" : "├─";
        String cross = node.crossesPage ? "†" : " ";
        String edgeHex = String.format("[%02x]", node.path[node.depth - 1] & 0xFF);

        out.print(prefix);
        out.print(connector);
        out.print(cross);
        out.print(edgeHex);
        out.print("─ ");
        out.println(nodeLabel(node));
    }

    private String nodeLabel(PartitionIndexWalker.NodeInfo node)
    {
        StringBuilder sb = new StringBuilder();

        String c = color ? nodeColor(node.nodeTypeName) : "";
        String r = color ? RESET : "";
        sb.append(c).append(node.nodeTypeName).append(r);
        sb.append(String.format(" @0x%x", node.position));

        if (node.childCount > 0)
            sb.append(String.format("  [%d %s]", node.childCount, node.childCount == 1 ? "child" : "children"));

        if (node.hasPayload)
        {
            if (node.isDataFile)
                sb.append(String.format("  → dfile:0x%x", ~node.payloadIndexPos));
            else
                sb.append(String.format("  → ifile:0x%x", node.payloadIndexPos));

            if (node.decodedKey != null)
                sb.append("  \"").append(node.decodedKey).append("\" (approx)");
        }

        return sb.toString();
    }

    private String nodeColor(String typeName)
    {
        if (typeName.startsWith("Payload")) return YELLOW;
        if (typeName.startsWith("Single"))  return GREEN;
        if (typeName.startsWith("Sparse"))  return BLUE;
        // Dense, LongDense
        return RED;
    }

    @Override
    public void end(boolean truncated)
    {
        if (truncated)
            out.println("\n[...traversal truncated by --max-nodes or --max-depth limit]");
    }
}
