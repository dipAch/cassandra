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
package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * nodetool paxostrace: stitched cross-node view of recent Paxos rounds.
 *
 * Each Paxos round spans multiple nodes (coordinator + replicas). Events are
 * keyed by ballot UUID, which is present in every message and is therefore the
 * natural cross-node correlation handle.
 *
 * Usage:
 *   nodetool paxostrace --table ks.t \
 *     --nodes 127.0.0.1:7100,127.0.0.2:7200,127.0.0.3:7300
 *
 * Without --nodes only the local node's events are shown (useful for quick
 * checks, but not a complete picture).
 */
@Command(name = "paxostrace",
         description = "Display a stitched cross-node Paxos round trace. " +
                       "Provide --nodes with the JMX address of every node to get a complete view.")
public class PaxosTrace extends AbstractCommand
{
    @Option(names = { "--table", "-t" },
            description = "Filter by keyspace.table (e.g. mykeyspace.mytable). Omit for all tables.",
            arity = "0..1")
    private String keyspaceTable = "";

    @Option(names = { "--limit", "-n" },
            description = "Maximum number of rounds to display, most-recent first (default: 20).",
            arity = "0..1")
    private int limit = 20;

    @Option(names = { "--nodes" },
            description = "Comma-separated host:jmxport pairs for every node in the cluster " +
                          "(e.g. 127.0.0.1:7100,127.0.0.2:7200,127.0.0.3:7300). " +
                          "The node nodetool is pointed at is always included automatically.",
            arity = "0..1")
    private String nodesArg = null;

    @Option(names = { "--latency", "-l" },
            description = "Print a per-phase latency breakdown after each ballot attempt. " +
                          "Metrics prefixed with ~ span multiple nodes and are approximate.",
            defaultValue = "false")
    private boolean showLatency;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        PrintStream err = probe.output().err;

        // --- 1. Collect events from all nodes ---
        List<String> allJsonEvents = new ArrayList<>(probe.getPaxosTraceEvents(keyspaceTable));

        if (nodesArg != null && !nodesArg.isEmpty())
        {
            String localHost = probe.getHost();
            int localPort = probe.getJmxPort();
            for (String spec : nodesArg.split(","))
            {
                spec = spec.trim();
                if (spec.isEmpty()) continue;
                String[] parts = spec.split(":");
                String host = parts[0].trim();
                int port = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : NodeProbe.defaultPort;

                // Already collected from the local node above
                if (host.equals(localHost) && port == localPort) continue;

                try (NodeProbe remote = new NodeProbe(host, port))
                {
                    allJsonEvents.addAll(remote.getPaxosTraceEvents(keyspaceTable));
                }
                catch (IOException e)
                {
                    err.printf("Warning: could not connect to %s:%d — %s%n", host, port, e.getMessage());
                }
            }
        }

        // V1 Paxos (legacy path) delivers messages to the local node via both the direct call
        // and the message queue, so the coordinator node records each replica event twice.
        // Deduplicate on (ballot, node, phase) — the same phase for the same ballot on the same
        // node is never legitimately emitted twice.
        {
            Set<String> seen = new HashSet<>();
            List<String> deduped = new ArrayList<>(allJsonEvents.size());
            for (String json : allJsonEvents)
            {
                Map<String, String> f = parseJson(json);
                String dk = f.get("ballot") + "|" + f.get("node") + "|" + f.get("phase");
                if (seen.add(dk))
                    deduped.add(json);
            }
            allJsonEvents = deduped;
        }

        if (allJsonEvents.isEmpty())
        {
            out.println("No Paxos trace events found.");
            if (nodesArg == null)
                out.println("Tip: pass --nodes host:jmxport,... to collect from all cluster nodes.");
            return;
        }

        // --- 2. Parse events and group by ballot ---
        // LinkedHashMap preserves insertion order; we'll sort the ballots after grouping.
        Map<String, List<Map<String, String>>> byBallot = new LinkedHashMap<>();
        for (String json : allJsonEvents)
        {
            Map<String, String> fields = parseJson(json);
            String ballot = fields.get("ballot");
            if (ballot == null) continue;
            byBallot.computeIfAbsent(ballot, k -> new ArrayList<>()).add(fields);
        }

        // --- 3. Sort ballot groups by earliest wallClockMs ---
        List<Map.Entry<String, List<Map<String, String>>>> ballotGroups =
            new ArrayList<>(byBallot.entrySet());
        ballotGroups.sort(Comparator.comparingLong(e -> minWallClock(e.getValue())));

        // Show the most-recent `limit` ballot groups
        int start = Math.max(0, ballotGroups.size() - limit);
        List<Map.Entry<String, List<Map<String, String>>>> visible =
            ballotGroups.subList(start, ballotGroups.size());

        // --- 4. Group ballot groups by roundId (coordinator-local grouping) ---
        // A roundId groups multiple ballot attempts that belong to one LWT call.
        // Build: roundId -> ordered list of ballot groups
        Map<String, List<Map.Entry<String, List<Map<String, String>>>>> byRound = new LinkedHashMap<>();
        for (Map.Entry<String, List<Map<String, String>>> bg : visible)
        {
            String roundId = extractRoundId(bg.getValue());
            String roundKey = roundId != null ? roundId : ("__ballot__" + bg.getKey());
            byRound.computeIfAbsent(roundKey, k -> new ArrayList<>()).add(bg);
        }

        // --- 5. Display ---
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        int roundNum = 1;
        for (Map.Entry<String, List<Map.Entry<String, List<Map<String, String>>>>> roundEntry
             : byRound.entrySet())
        {
            List<Map.Entry<String, List<Map<String, String>>>> attempts = roundEntry.getValue();
            String roundId = roundEntry.getKey().startsWith("__ballot__")
                             ? null : roundEntry.getKey();
            long firstMs = attempts.isEmpty() ? 0 : minWallClock(attempts.get(0).getValue());
            String outcome = roundOutcome(attempts);

            if (roundId != null)
                out.printf("Round %d  [roundId=%s]  %s  %s  (%d ballot attempt(s))%n",
                           roundNum++, shortId(roundId), sdf.format(new Date(firstMs)),
                           outcome, attempts.size());
            else
                out.printf("Round %d  %s  %s%n",
                           roundNum++, sdf.format(new Date(firstMs)), outcome);

            for (int a = 0; a < attempts.size(); a++)
            {
                Map.Entry<String, List<Map<String, String>>> bg = attempts.get(a);
                String ballot = bg.getKey();
                List<Map<String, String>> events = new ArrayList<>(bg.getValue());
                events.sort(Comparator.comparingLong(e -> wallClock(e)));

                if (attempts.size() > 1)
                    out.printf("  Ballot %s  [attempt %d]%n", shortId(ballot), a + 1);
                else
                    out.printf("  Ballot %s%n", shortId(ballot));

                for (Map<String, String> ev : events)
                {
                    String ts  = sdf.format(new Date(wallClock(ev)));
                    String role  = padRight(ev.getOrDefault("role",  "?"), 11);
                    String node  = padRight(ev.getOrDefault("node",  "?"), 15);
                    String phase = padRight(ev.getOrDefault("phase", "?"), 20);
                    String extra = formatExtra(ev);
                    out.printf("    %s  %s  %s  %s  %s%n", ts, role, node, phase, extra);
                }
                if (showLatency)
                {
                    String latency = computeLatencyBreakdown(events);
                    if (!latency.isEmpty())
                        out.println(latency);
                }
                out.println();
            }
        }

        out.printf("[%d rounds shown, %d total ballot groups across all nodes]%n",
                   byRound.size(), ballotGroups.size());
    }

    // ---- helpers ----

    private static String roundOutcome(List<Map.Entry<String, List<Map<String, String>>>> attempts)
    {
        // COMMITTED: explicit commit event seen
        for (Map.Entry<String, List<Map<String, String>>> bg : attempts)
            for (Map<String, String> ev : bg.getValue())
                if ("COMMIT_DONE".equals(ev.get("phase")) || "COMMIT".equals(ev.get("phase")))
                    return "COMMITTED";
        // ACCEPTED without COMMIT_DONE: the proposed update was empty (condition failed).
        // commitPaxos() is skipped entirely for empty updates — no commit is sent at all,
        // sync or async. beginAndRepairPaxos() also skips replaying empty in-progress updates,
        // so correctness is preserved without a commit message.
        for (Map.Entry<String, List<Map<String, String>>> bg : attempts)
            for (Map<String, String> ev : bg.getValue())
                if ("PROPOSE_DONE".equals(ev.get("phase")) && "ACCEPTED".equals(ev.get("outcome")))
                    return "ACCEPTED (commit skipped)";
        // PREPARE_QUORUM_FAILED: every event is a REJECT at the prepare phase —
        // the coordinator never reached a quorum of promises (orphan ballot).
        boolean hasNonReject = false;
        for (Map.Entry<String, List<Map<String, String>>> bg : attempts)
            for (Map<String, String> ev : bg.getValue())
            {
                String phase = ev.get("phase");
                if (phase != null && !phase.endsWith("_REJECT") && !"PREPARE_REJECT".equals(phase)
                    && !"LEGACY_PREPARE_REJECT".equals(phase))
                {
                    hasNonReject = true;
                    break;
                }
            }
        if (!hasNonReject)
            return "PREPARE_QUORUM_FAILED";
        return "IN-FLIGHT";
    }

    private static String extractRoundId(List<Map<String, String>> events)
    {
        for (Map<String, String> ev : events)
        {
            String rid = ev.get("roundId");
            if (rid != null && !rid.isEmpty()) return rid;
        }
        return null;
    }

    private static long minWallClock(List<Map<String, String>> events)
    {
        return events.stream().mapToLong(PaxosTrace::wallClock).min().orElse(0);
    }

    private static long wallClock(Map<String, String> ev)
    {
        try { return Long.parseLong(ev.getOrDefault("wallClockMs", "0")); }
        catch (NumberFormatException e) { return 0; }
    }

    private static String shortId(String uuid)
    {
        return uuid.length() > 8 ? uuid.substring(0, 8) : uuid;
    }

    private static String padRight(String s, int width)
    {
        if (s.length() >= width) return s;
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < width) sb.append(' ');
        return sb.toString();
    }

    private static String formatExtra(Map<String, String> ev)
    {
        StringBuilder sb = new StringBuilder();
        String outcome           = ev.get("outcome");
        String supersededBy      = ev.get("supersededBy");
        String snapshotPromised  = ev.get("snapshotPromised");
        String snapshotAccepted  = ev.get("snapshotAccepted");
        String snapshotCommitted = ev.get("snapshotCommitted");
        if (outcome != null && !outcome.isEmpty())
            sb.append("outcome=").append(outcome);
        if (supersededBy != null && !supersededBy.isEmpty())
            appendExtra(sb, "supersededBy=" + shortId(supersededBy));
        if (snapshotPromised != null && !snapshotPromised.isEmpty())
            appendExtra(sb, "prev_promised=" + shortId(snapshotPromised));
        if (snapshotAccepted != null && !snapshotAccepted.isEmpty())
            appendExtra(sb, "prev_accepted=" + shortId(snapshotAccepted));
        if (snapshotCommitted != null && !snapshotCommitted.isEmpty())
            appendExtra(sb, "prev_committed=" + shortId(snapshotCommitted));
        return sb.toString();
    }

    private static void appendExtra(StringBuilder sb, String s)
    {
        if (sb.length() > 0) sb.append("  ");
        sb.append(s);
    }

    private static String computeLatencyBreakdown(List<Map<String, String>> events)
    {
        long minTs = Long.MAX_VALUE, maxTs = Long.MIN_VALUE;
        long tPrepareDone = 0, tProposeDone = 0, tCommitDone = 0, tFirstPropose = 0;

        for (Map<String, String> ev : events)
        {
            long ts = wallClock(ev);
            if (ts <= 0) continue;
            if (ts < minTs) minTs = ts;
            if (ts > maxTs) maxTs = ts;
            switch (ev.getOrDefault("phase", ""))
            {
                case "PREPARE_DONE":                   tPrepareDone = ts; break;
                case "PROPOSE_DONE":                   tProposeDone = ts; break;
                case "COMMIT_DONE":                    tCommitDone  = ts; break;
                case "PROPOSE": case "LEGACY_PROPOSE":
                    if (tFirstPropose == 0 || ts < tFirstPropose) tFirstPropose = ts;
                    break;
            }
        }

        if (minTs == Long.MAX_VALUE || tPrepareDone == 0) return "";

        StringBuilder sb = new StringBuilder("  Latency:");
        sb.append("  prepare=~").append(tPrepareDone - minTs).append("ms");
        if (tFirstPropose > tPrepareDone)
            sb.append("  check=~").append(tFirstPropose - tPrepareDone).append("ms");
        if (tProposeDone > 0)
        {
            long base = tFirstPropose > tPrepareDone ? tFirstPropose : tPrepareDone;
            sb.append("  propose=~").append(tProposeDone - base).append("ms");
        }
        if (tCommitDone > 0 && tProposeDone > 0)
            sb.append("  commit=").append(tCommitDone - tProposeDone).append("ms");
        if (maxTs > minTs)
            sb.append("  total=").append(maxTs - minTs).append("ms");
        return sb.toString();
    }

    /**
     * Minimal JSON field extractor, no external dependencies.
     * Handles the flat string-value JSON produced by PaxosTraceEvent.toJson().
     * Numeric fields (wallClockMs, ballotMicros) are extracted as strings too.
     */
    static Map<String, String> parseJson(String json)
    {
        Map<String, String> result = new LinkedHashMap<>();
        int i = 0;
        int len = json.length();
        while (i < len)
        {
            // find next "key"
            int ks = json.indexOf('"', i);
            if (ks < 0) break;
            int ke = json.indexOf('"', ks + 1);
            if (ke < 0) break;
            String key = json.substring(ks + 1, ke);
            i = ke + 1;

            // skip to ':'
            int colon = json.indexOf(':', i);
            if (colon < 0) break;
            i = colon + 1;

            // skip whitespace
            while (i < len && json.charAt(i) == ' ') i++;

            String value;
            if (i < len && json.charAt(i) == '"')
            {
                // string value: handle \" escapes
                int vs = i + 1;
                int ve = vs;
                while (ve < len)
                {
                    char c = json.charAt(ve);
                    if (c == '\\') { ve += 2; continue; }
                    if (c == '"') break;
                    ve++;
                }
                value = json.substring(vs, ve).replace("\\\"", "\"").replace("\\\\", "\\");
                i = ve + 1;
            }
            else
            {
                // numeric value
                int vs = i;
                while (i < len && json.charAt(i) != ',' && json.charAt(i) != '}') i++;
                value = json.substring(vs, i).trim();
            }

            result.put(key, value);
        }
        return result;
    }
}
