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

package org.apache.cassandra.db;

import org.junit.After;
//import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;

/**
 * Repro for a bug where a row re-inserted at the exact start of a range tombstone
 * disappears after major compaction.
 *
 * Reproduces with LeveledCompactionStrategy and gc_grace_seconds=86400 (the default).
 *
 *   1. Insert NUM_ROWS rows (ck = "row000000".."rowNNNNNN") into SSTable #1.
 *   2. Delete  ck >= TOMBSTONE_CK  (open-ended; sorts after all "row*" keys).
 *   3. Re-insert TOMBSTONE_CK with a timestamp newer than the delete.
 *   4. Force major compaction.
 *   5. Range-scan ... AND ck <= TOMBSTONE_CK -- the re-inserted row disappears.
 */
//public class RangeTombstoneBoundaryReproTest extends CQLTester
//{
//    // Padding makes each row large enough that NUM_ROWS rows span multiple 1 KiB
//    // column-index blocks.  With ~300-byte rows and 1 KiB blocks, ~3 rows/block;
//    // NUM_ROWS = 9 is the minimum: 8 rows pass cleanly.
//    private static final String VALUE_PAD = "x".repeat(270);
//    private static final int    NUM_ROWS  = 9;
//
//    // Tombstone boundary: sorts after every "row*" key.
//    private static final String TOMBSTONE_CK = "z";
//
//    @Test
//    public void testReinsertedRowAtTombstoneBoundaryMissingAfterCompaction() throws Throwable
//    {
//        int savedIdx   = DatabaseDescriptor.getColumnIndexSizeInKiB();
//        int savedCache = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
//        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
//        DatabaseDescriptor.setColumnIndexCacheSize(1);
//        try
//        {
//            createTable(
//            "CREATE TABLE %s (" +
//            "  pk ascii, ck ascii, v text," +
//            "  PRIMARY KEY (pk, ck)" +
//            ") WITH" +
//            " compaction = {" +
//            "        'class': 'LeveledCompactionStrategy'," +
//            "        'enabled': false" +
//            "      }"
//            );
//
//            final String PK = "p";
//
//            // SSTable 1: bulk insert
//            for (int i = 0; i < NUM_ROWS; i++)
//                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP ?",
//                        PK, String.format("row%06d", i), VALUE_PAD + i, (long) i);
//            flush();
//
//            // SSTable 2: open-ended tombstone starting at TOMBSTONE_CK
//            execute("DELETE FROM %s USING TIMESTAMP 100 WHERE pk = ? AND ck >= ?",
//                    PK, TOMBSTONE_CK);
//            flush();
//
//            // SSTable 3: re-insert at tombstone boundary with newer timestamp
//            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 200",
//                    PK, TOMBSTONE_CK, "reinserted");
//            flush();
//
//            // Sanity: row must exist before compaction
//            UntypedResultSet before = execute(
//            "SELECT ck FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?",
//            PK, "s", TOMBSTONE_CK);
//            Assert.assertEquals("Row must exist before compaction", 1, before.size());
//
//            getCurrentColumnFamilyStore().forceMajorCompaction(); // Comment / uncomment this line to make the test pass
//
//            // BUG: re-inserted row disappears after compaction
//            UntypedResultSet after = execute(
//            "SELECT ck FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?",
//            PK, "s", TOMBSTONE_CK);
//            Assert.assertEquals(
//            "Re-inserted row at tombstone boundary must survive compaction",
//            1, after.size());
//        }
//        finally
//        {
//            DatabaseDescriptor.setColumnIndexSizeInKiB(savedIdx);
//            DatabaseDescriptor.setColumnIndexCacheSize(savedCache);
//        }
//    }
//}

public class RangeTombstoneBoundaryReproTest extends CQLTester
{
    private int savedColumnIndexSizeInKiB;
    private int savedColumnIndexCacheSizeInKiB;

    @Before
    public void saveConfig()
    {
        savedColumnIndexSizeInKiB = DatabaseDescriptor.getColumnIndexSizeInKiB();
        savedColumnIndexCacheSizeInKiB = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        DatabaseDescriptor.setColumnIndexCacheSize(1);
    }

    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.setColumnIndexSizeInKiB(savedColumnIndexSizeInKiB);
        DatabaseDescriptor.setColumnIndexCacheSize(savedColumnIndexCacheSizeInKiB);
    }

    private ColumnFamilyStore getCfs()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
    }

    @Test
    public void testRangeSelectReturnsEmptyAfterCompaction()
    {
        createTable("CREATE TABLE %s (pk text, ck text, v1 text, PRIMARY KEY (pk, ck))" +
                    " WITH gc_grace_seconds = 0" +
                    " AND compression = {'enabled': 'false'}" +
                    " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");

        // 30 rows with ~100-byte values → ~3 index blocks at 1 KiB each
        String pad = "x".repeat(80);
        for (int i = 0; i < 30; i++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)",
                    "p", String.format("r%03d", i), pad + i);

        getCfs().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // Open-ended delete: removes rows 26-29 (last block)
        execute("DELETE FROM %s WHERE pk = ? AND ck >= ?", "p", "r026");

        getCfs().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        getCfs().forceMajorCompaction();

        // Partition scan sees all 26 surviving rows — data is present
        UntypedResultSet all = execute("SELECT * FROM %s WHERE pk = ?", "p");
        assertEquals("partition scan should see 26 rows", 26, all.size());

        // BUG: range select near end returns 0 instead of 3
        UntypedResultSet range = execute(
        "SELECT * FROM %s WHERE pk = ? AND ck >= ? AND ck <= ?",
        "p", "r023", "r029");
        assertEquals("range select [r023,r029] should return 3 rows (23,24,25)",
                     3, range.size());
    }
}
