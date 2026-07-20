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

import javax.annotation.Nullable;

/**
 * Immutable snapshot of a single Paxos phase event on one node.
 *
 * All fields are primitives or Strings so the object holds no references
 * to live Cassandra data structures after construction.
 *
 * Phases emitted by the REPLICA role:
 *   PREPARE               : promise granted (promiseIfNewer → PROMISE)
 *   PERMIT_READ           : read-only permit granted (promiseIfNewer → PERMIT_READ)
 *   PREPARE_REJECT        : promise refused (promiseIfNewer → REJECT)
 *   PROPOSE               : proposal accepted (acceptIfLatest → SUCCESS)
 *   PROPOSE_REJECT        : proposal refused (acceptIfLatest → superseded)
 *   COMMIT                : commit applied (applyCommit)
 *   LEGACY_PREPARE        : v1 promise granted
 *   LEGACY_PREPARE_REJECT : v1 promise refused
 *   LEGACY_PROPOSE        : v1 proposal accepted
 *   LEGACY_PROPOSE_REJECT : v1 proposal refused
 *
 * Phases emitted by the COORDINATOR role (StorageProxy.doPaxos):
 *   PREPARE_DONE   : Prepare phase succeeded, ballot chosen
 *   PROPOSE_DONE   : Propose phase completed (outcome: ACCEPTED | REJECTED)
 *   COMMIT_DONE    : Commit dispatched
 */
public class PaxosTraceEvent
{
    public final String node;
    public final String role;          // "COORDINATOR" | "REPLICA"
    public final String phase;
    public final String ballot;        // Ballot.toString()
    public final long   ballotMicros;  // Ballot.unixMicros()
    public final String keyspace;
    public final String table;
    public final String partitionHex;  // ByteBufferUtil.bytesToHex(key.getKey())
    public final String token;         // key.getToken().toString()
    public final long   wallClockMs;

    @Nullable public final String roundId;           // non-null on COORDINATOR events only
    @Nullable public final String outcome;           // phase-specific outcome label, null for COMMIT / COMMIT_DONE
    @Nullable public final String supersededBy;      // ballot string of the winner on REJECT events

    // Replica state-machine snapshot captured before this operation ran.
    // Null on COORDINATOR events and on COMMIT when state is unavailable.
    @Nullable public final String snapshotPromised;  // before.promised.toString()
    @Nullable public final String snapshotAccepted;  // before.accepted.ballot.toString(), or null if none
    @Nullable public final String snapshotCommitted; // before.committed.ballot.toString()

    public PaxosTraceEvent(String node,
                           String role,
                           String phase,
                           String ballot,
                           long ballotMicros,
                           String keyspace,
                           String table,
                           String partitionHex,
                           String token,
                           long wallClockMs,
                           @Nullable String roundId,
                           @Nullable String outcome,
                           @Nullable String supersededBy,
                           @Nullable String snapshotPromised,
                           @Nullable String snapshotAccepted,
                           @Nullable String snapshotCommitted)
    {
        this.node = node;
        this.role = role;
        this.phase = phase;
        this.ballot = ballot;
        this.ballotMicros = ballotMicros;
        this.keyspace = keyspace;
        this.table = table;
        this.partitionHex = partitionHex;
        this.token = token;
        this.wallClockMs = wallClockMs;
        this.roundId = roundId;
        this.outcome = outcome;
        this.supersededBy = supersededBy;
        this.snapshotPromised = snapshotPromised;
        this.snapshotAccepted = snapshotAccepted;
        this.snapshotCommitted = snapshotCommitted;
    }

    public String toJson()
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        kv(sb, "node", node);         sb.append(',');
        kv(sb, "role", role);         sb.append(',');
        kv(sb, "phase", phase);       sb.append(',');
        kv(sb, "ballot", ballot);     sb.append(',');
        sb.append("\"ballotMicros\":").append(ballotMicros); sb.append(',');
        kv(sb, "keyspace", keyspace); sb.append(',');
        kv(sb, "table", table);       sb.append(',');
        kv(sb, "partitionHex", partitionHex); sb.append(',');
        kv(sb, "token", token);       sb.append(',');
        sb.append("\"wallClockMs\":").append(wallClockMs);
        if (roundId != null)           { sb.append(','); kv(sb, "roundId", roundId); }
        if (outcome != null)           { sb.append(','); kv(sb, "outcome", outcome); }
        if (supersededBy != null)      { sb.append(','); kv(sb, "supersededBy", supersededBy); }
        if (snapshotPromised != null)  { sb.append(','); kv(sb, "snapshotPromised",  snapshotPromised); }
        if (snapshotAccepted != null)  { sb.append(','); kv(sb, "snapshotAccepted",  snapshotAccepted); }
        if (snapshotCommitted != null) { sb.append(','); kv(sb, "snapshotCommitted", snapshotCommitted); }
        sb.append('}');
        return sb.toString();
    }

    private static void kv(StringBuilder sb, String key, String val)
    {
        sb.append('"').append(key).append("\":\"");
        if (val != null)
            sb.append(val.replace("\\", "\\\\").replace("\"", "\\\""));
        sb.append('"');
    }
}
