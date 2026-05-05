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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.bti.PartitionIndex;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Depth-first walker over a BTI partition index trie. Extends {@link PartitionIndex.Reader}
 * to access the protected Walker API. Streams {@link NodeInfo} records to a {@link TrieViewVisitor}.
 *
 * <p>Pre-reads all child transitions at each node before recursing, so the walker's internal
 * buffer state does not need to be restored between siblings — making the traversal safe
 * regardless of page boundaries.
 */
public class PartitionIndexWalker extends PartitionIndex.Reader
{
    /** Snapshot of a single node emitted to the visitor during DFS traversal. */
    public static class NodeInfo
    {
        /** File position of this node. */
        public final long position;
        /** Node type name from {@link org.apache.cassandra.io.tries.TrieNode#toString()}, e.g. {@code "Sparse16"}. */
        public final String nodeTypeName;
        /** Depth in the trie; 0 = root. Also equals {@code path.length}. */
        public final int depth;
        /** Transition bytes from root to this node (one byte per depth level). */
        public final byte[] path;
        /** True if this node carries a payload (i.e. is a key terminus). */
        public final boolean hasPayload;
        /**
         * File position decoded from the payload, or {@link PartitionIndex#NOT_FOUND} if none.
         * Negative (other than NOT_FOUND) means {@code ~value} points into the data file directly.
         * Non-negative means it points into the row index file.
         */
        public final long payloadIndexPos;
        /** True when {@link #payloadIndexPos} encodes a data-file pointer (use {@code ~payloadIndexPos}). */
        public final boolean isDataFile;
        /** True if the edge from parent to this node crosses a 4 KiB page boundary (cache miss indicator). */
        public final boolean crossesPage;
        /** Number of non-null children (may be less than the node's transition range for DENSE nodes). */
        public final int childCount;
        /** True if this is the last non-null child of its parent (used for box-drawing connectors). */
        public final boolean isLastChild;
        /** File position of this node's parent; {@code -1} for the root. */
        public final long parentPosition;
        /**
         * Best-effort human-readable key decoded from the trie path bytes.
         * Set by the walker only when {@code --decode-keys} is active and this node has a payload.
         * Null otherwise. Note: the BTI index stores unique key <em>prefixes</em>, so the decoded
         * value is approximate and labelled accordingly.
         */
        public String decodedKey;

        NodeInfo(long position, String nodeTypeName, int depth, byte[] path,
                 boolean hasPayload, long payloadIndexPos, boolean crossesPage,
                 int childCount, boolean isLastChild, long parentPosition)
        {
            this.position = position;
            this.nodeTypeName = nodeTypeName;
            this.depth = depth;
            this.path = path;
            this.hasPayload = hasPayload;
            this.payloadIndexPos = payloadIndexPos;
            this.isDataFile = hasPayload && payloadIndexPos < 0 && payloadIndexPos != PartitionIndex.NOT_FOUND;
            this.crossesPage = crossesPage;
            this.childCount = childCount;
            this.isLastChild = isLastChild;
            this.parentPosition = parentPosition;
        }
    }

    private byte[] pathBuf = new byte[64];
    private int pathLen = 0;
    private int totalNodes = 0;
    private boolean truncated = false;

    private final int maxNodes;
    private final int maxDepth;
    private final IPartitioner partitioner;
    private final boolean decodeKeys;

    public PartitionIndexWalker(PartitionIndex index, int maxNodes, int maxDepth,
                                IPartitioner partitioner, boolean decodeKeys)
    {
        super(index);
        this.maxNodes = maxNodes;
        this.maxDepth = maxDepth;
        this.partitioner = partitioner;
        this.decodeKeys = decodeKeys;
    }

    public boolean isTruncated()
    {
        return truncated;
    }

    public int getTotalNodes()
    {
        return totalNodes;
    }

    /** Starts a DFS traversal from the trie root, streaming each node to the visitor. */
    public void walk(TrieViewVisitor visitor)
    {
        dfs(root, -1L, 0, visitor, true);
    }

    private void dfs(long nodePos, long parentPos, int depth, TrieViewVisitor visitor, boolean isLastChild)
    {
        go(nodePos);

        // Snapshot ALL state from this node before any recursive go() call changes the buffer.
        String typeName = nodeType.toString();
        int range = transitionRange();
        boolean hasP = hasPayload();
        long indexPos = hasP ? getCurrentIndexPos() : PartitionIndex.NOT_FOUND;
        boolean crossesPage = parentPos >= 0 && PageAware.pageStart(parentPos) != PageAware.pageStart(nodePos);

        // Pre-read transitions so we don't need to call go(nodePos) again between siblings.
        int[] transBytes = new int[range];
        long[] childPositions = new long[range];
        int childCount = 0;
        int lastNonNoneIdx = -1;
        for (int i = 0; i < range; i++)
        {
            transBytes[i] = transitionByte(i);
            childPositions[i] = transition(i);
            if (childPositions[i] != NONE)
            {
                childCount++;
                lastNonNoneIdx = i;
            }
        }

        byte[] path = Arrays.copyOf(pathBuf, pathLen);
        NodeInfo info = new NodeInfo(nodePos, typeName, depth, path, hasP, indexPos,
                                     crossesPage, childCount, isLastChild, parentPos);

        if (decodeKeys && hasP && partitioner != null)
            info.decodedKey = tryDecodeKey(path);

        visitor.visitNode(info);
        totalNodes++;

        if ((maxNodes > 0 && totalNodes >= maxNodes) || (maxDepth > 0 && depth >= maxDepth))
        {
            truncated = true;
            return;
        }

        for (int i = 0; i < range; i++)
        {
            if (childPositions[i] == NONE || truncated) continue;

            if (pathLen >= pathBuf.length)
                pathBuf = Arrays.copyOf(pathBuf, pathBuf.length * 2);
            pathBuf[pathLen++] = (byte) transBytes[i];

            dfs(childPositions[i], nodePos, depth + 1, visitor, i == lastNonNoneIdx);

            pathLen--;
        }
    }

    private String tryDecodeKey(byte[] pathBytes)
    {
        if (pathBytes.length == 0) return null;
        try
        {
            final byte[] copy = pathBytes;
            ByteComparable bc = v -> ByteSource.fixedLength(copy, 0, copy.length);
            DecoratedKey key = BufferDecoratedKey.fromByteComparable(bc, BYTE_COMPARABLE_VERSION, partitioner);
            ByteBuffer keyBytes = key.getKey();
            if (!keyBytes.hasRemaining()) return null;
            byte[] bytes = new byte[keyBytes.remaining()];
            keyBytes.duplicate().get(bytes);
            // Show as UTF-8 if all printable ASCII; otherwise hex
            boolean printable = true;
            for (byte b : bytes)
                if (b < 0x20 || b > 0x7e) { printable = false; break; }
            return printable ? new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
                             : "0x" + bytesToHex(bytes);
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static String bytesToHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes)
            sb.append(String.format("%02x", b & 0xFF));
        return sb.toString();
    }
}
