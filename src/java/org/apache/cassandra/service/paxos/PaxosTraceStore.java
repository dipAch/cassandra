/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.service.paxos;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Always-on, in-memory store for Paxos trace events on this node.
 *
 * Events are keyed by "keyspace.table" and stored in per-table bounded
 * deques (oldest-first). Total event count is capped at maxEvents.
 *
 * Thread safety: bucket creation uses ConcurrentHashMap.computeIfAbsent;
 * per-bucket reads/writes use synchronized(deque).
 *
 * The totalCount <:AtomicInteger:> is an approximate counter. Momentary over-capacity by
 * the number of concurrent writers is acceptable.
 *
 * Exposed as a JMX MBean (registered in PaxosState.initializeTrackers).
 */
public class PaxosTraceStore implements PaxosTraceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.service:type=PaxosTrace";
    public static final PaxosTraceStore instance = new PaxosTraceStore();

    private final ConcurrentHashMap<String, ArrayDeque<PaxosTraceEvent>> eventsByTable = new ConcurrentHashMap<>();
    private volatile int maxEvents = 50_000;
    private final AtomicInteger totalCount = new AtomicInteger(0);

    private PaxosTraceStore() {}

    public void add(PaxosTraceEvent event)
    {
        String key = event.keyspace + '.' + event.table;
        ArrayDeque<PaxosTraceEvent> deque = eventsByTable.computeIfAbsent(key, k -> new ArrayDeque<>(256));
        synchronized (deque)
        {
            if (totalCount.get() >= maxEvents)
            {
                if (!deque.isEmpty())
                {
                    deque.pollFirst();
                    totalCount.decrementAndGet();
                }
                else
                {
                    // This table has no events yet; skip rather than evicting from another table.
                    return;
                }
            }
            deque.addLast(event);
            totalCount.incrementAndGet();
        }
    }

    @Override
    public List<String> getEvents(String keyspaceTable)
    {
        List<String> result = new ArrayList<>();
        if (keyspaceTable == null || keyspaceTable.isEmpty())
        {
            for (ArrayDeque<PaxosTraceEvent> deque : eventsByTable.values())
            {
                synchronized (deque)
                {
                    for (PaxosTraceEvent e : deque)
                        result.add(e.toJson());
                }
            }
        }
        else
        {
            ArrayDeque<PaxosTraceEvent> deque = eventsByTable.get(keyspaceTable);
            if (deque != null)
            {
                synchronized (deque)
                {
                    for (PaxosTraceEvent e : deque)
                        result.add(e.toJson());
                }
            }
        }
        return result;
    }

    @Override
    public void clear()
    {
        eventsByTable.clear();
        totalCount.set(0);
    }

    @Override
    public int getMaxEvents()
    {
        return maxEvents;
    }

    @Override
    public void setMaxEvents(int max)
    {
        this.maxEvents = max;
    }
}
