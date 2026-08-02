package com.progetto.gossip;

import com.progetto.rmi.GossipRemote;
import com.progetto.rmi.WorkerRemote;

import java.rmi.RemoteException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns load-balancing state for one worker: its own versioned load and the
 * gossiped load of its peers. Deliberately does NOT own membership or peer
 * stubs — that's Worker.peers, populated by the dynamic join protocol
 * (registerPeer/joinNetwork). GossipService is handed that same live map
 * so it always gossips with whoever is currently known, without keeping a
 * separate, potentially stale, copy.
 */
public class GossipService {

    private static final int GOSSIP_INTERVAL_SECONDS = 2;
    /** How much lower a peer's load must be before we forward instead of running locally. */
    private static final int LOAD_IMBALANCE_THRESHOLD = 2;

    private final String workerId;
    private final ConcurrentHashMap<String, WorkerRemote> peers;
    private final ClusterState clusterState = new ClusterState();
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler;

    // Only ever touched inside the synchronized recordLocalLoadChange(), so plain fields suffice.
    private long versionCounter = 0;
    private int localLoad = 0;

    public GossipService(String workerId, ConcurrentHashMap<String, WorkerRemote> peers) {
        this.workerId = workerId;
        this.peers = peers;

        versionCounter++;
        clusterState.merge(new WorkerView(workerId, 0, versionCounter, System.currentTimeMillis()));

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gossip-" + workerId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::gossipRound, GOSSIP_INTERVAL_SECONDS, GOSSIP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /** Called by Worker whenever a locally-executing job is enqueued (+1) or finishes (-1). */
    public synchronized void recordLocalLoadChange(int delta) {
        localLoad += delta;
        versionCounter++;
        WorkerView current = clusterState.get(workerId);
        clusterState.merge(current.withLoad(localLoad, versionCounter));
    }

    /** Returns a peer to forward to if it's significantly less loaded than us, else null (run locally). */
    public String decideForwardTarget() {
        // Filtered directly against `peers` (not just excluding self): ClusterState can carry a
        // "ghost" entry for a worker we've locally evicted but a third peer still gossips about —
        // picking the least-loaded among only currently-known peers avoids getting stuck offering
        // an unreachable candidate instead of falling through to the next-best real one.
        String candidateId = clusterState.allViews().stream()
                .filter(v -> !v.getWorkerId().equals(workerId))
                .filter(v -> peers.containsKey(v.getWorkerId()))
                .min(Comparator.comparingInt(WorkerView::getLoadCount))
                .map(WorkerView::getWorkerId)
                .orElse(null);
        if (candidateId == null) {
            return null;
        }
        WorkerView self = clusterState.get(workerId);
        WorkerView candidate = clusterState.get(candidateId);
        if (self.getLoadCount() - candidate.getLoadCount() > LOAD_IMBALANCE_THRESHOLD) {
            return candidateId;
        }
        return null;
    }

    public ClusterState exchangeState(ClusterState callerState) {
        clusterState.mergeAll(callerState);
        return clusterState.snapshot();
    }

    /** Drops a peer from the gossiped load view, e.g. after evicting it from membership on RPC failure. */
    public void forgetPeer(String peerId) {
        clusterState.remove(peerId);
    }

    private void gossipRound() {
        List<String> candidates = new ArrayList<>(peers.keySet());
        if (candidates.isEmpty()) {
            return;
        }
        String target = candidates.get(random.nextInt(candidates.size()));
        try {
            WorkerRemote stub = peers.get(target);
            if (stub == null) {
                return; // evicted concurrently by another operation
            }
            GossipRemote gossipStub = (GossipRemote) stub;
            ClusterState outgoing = clusterState.snapshot();
            log("round -> " + target + " (sending " + outgoing.size() + " views)");
            ClusterState response = gossipStub.exchangeState(outgoing);
            clusterState.mergeAll(response);
            log("round <- " + target + " merged, cluster now: " + describeCluster());
        } catch (RemoteException e) {
            log("round with " + target + " FAILED: " + e.getMessage() + " -- evicting peer");
            peers.remove(target);
            forgetPeer(target);
        }
    }

    private String describeCluster() {
        StringBuilder sb = new StringBuilder();
        clusterState.allViews().forEach(v ->
                sb.append(v.getWorkerId()).append("(load=").append(v.getLoadCount())
                        .append(",v=").append(v.getVersion()).append(") "));
        return sb.toString().trim();
    }

    private void log(String message) {
        System.out.printf("[%s] [gossip=%s] %s%n", LocalDateTime.now(), workerId, message);
    }
}
