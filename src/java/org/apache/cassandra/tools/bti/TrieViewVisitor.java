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

import java.io.Closeable;
import java.io.IOException;

/**
 * Visitor interface for streaming DFS traversal of a BTI partition index trie.
 * Renderers implement this interface to produce different output formats.
 */
public interface TrieViewVisitor extends Closeable
{
    /**
     * Called once before any nodes are visited.
     *
     * @param keyCount  total number of keys in the index
     * @param firstName decoded first key, or null
     * @param lastName  decoded last key, or null
     */
    void begin(long keyCount, String firstName, String lastName);

    /**
     * Called once per node in DFS pre-order.
     */
    void visitNode(PartitionIndexWalker.NodeInfo node);

    /**
     * Called once after all nodes have been visited.
     *
     * @param truncated true if traversal was cut short by a --max-nodes or --max-depth limit
     */
    void end(boolean truncated) throws IOException;

    @Override
    default void close() throws IOException {}
}
