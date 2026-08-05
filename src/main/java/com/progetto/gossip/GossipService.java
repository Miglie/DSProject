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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    /**Failure detection treshold */
    private static final int CRASHED_WORKER_TRESHOLD_MS = 6000;

    private final String workerId;
    private final ConcurrentHashMap<String, WorkerRemote> peers;
    private final ClusterState clusterState = new ClusterState();
    /** peerId -> jobs forwarded to it that are still in flight; needed because sequential jobs may be sent
     * to the same peer without taking into account the one sent prevoiusly, until gossip update
    */
    private final ConcurrentHashMap<String, Integer> pendingForwards = new ConcurrentHashMap<>();
    private final Random random = new Random();
    //This executes the scheduled target selection for gossip and failure detection -> does not do the concrete
    //gossip exchange, it was blocking and was paralizing the network
    private final ScheduledExecutorService scheduler;
    //Real gossip is executed concurrently to avoid RMI delays in case of mute Socket
    private final ExecutorService gossipExecutor;

    private volatile long versionCounter = 0;
    private volatile int localLoad = 0;

    public GossipService(String workerId, ConcurrentHashMap<String, WorkerRemote> peers) {
        this.workerId = workerId;
        this.peers = peers;

        versionCounter++;
        clusterState.merge(new WorkerView(workerId, 0, versionCounter, System.currentTimeMillis()));

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gossip-timer" + workerId);
            t.setDaemon(true);
            return t;
        });

        this.gossipExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "gossip-io-" + workerId);
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

    /**
     * Picks a peer to forward to if it is significantly less loaded than this worker and reserves
     * a slot on it, or returns null to run the job locally.
     */
    public synchronized String reserveForwardTarget() {
        WorkerView self = clusterState.get(workerId);
        String target = clusterState.allViews().stream()
                .filter(v -> !v.getWorkerId().equals(workerId))
                .filter(v -> peers.containsKey(v.getWorkerId()))
                .min(Comparator.comparingInt(this::effectiveLoad))
                .filter(candidate -> self.getLoadCount() - effectiveLoad(candidate) > LOAD_IMBALANCE_THRESHOLD)
                .map(WorkerView::getWorkerId)
                .orElse(null);
        if (target != null) {
            pendingForwards.merge(target, 1, Integer::sum);
        }
        return target;
    }

    /** Releases a slot once the forward has settled. */
    public void releaseForwardSlot(String peerId) {
        pendingForwards.computeIfPresent(peerId, (id, count) -> count == 1 ? null : count - 1);
    }

    /** A peer's gossiped load plus the jobs we've sent it that it hasn't gossiped back to us yet. */
    private int effectiveLoad(WorkerView view) {
        return view.getLoadCount() + pendingForwards.getOrDefault(view.getWorkerId(), 0);
    }

    public ClusterState exchangeState(ClusterState callerState) {
        clusterState.mergeAll(callerState, this.workerId, this.peers);
        return clusterState.snapshot();
    }

    /** Drops a peer from the gossiped load view, e.g. after evicting it from membership on RPC failure. */
    public void forgetPeer(String peerId) {
        clusterState.remove(peerId);
        pendingForwards.remove(peerId);
    }

    /**Bootstrap first cluster view already up to date from seed node */
    public void bootstrap(GossipRemote seed, String targetWorkerId) {
        try{
            ClusterState response = seed.exchangeState(this.clusterState.snapshot());
            clusterState.mergeAll(response, this.workerId, this.peers);
            log("bootstrap round <- " + targetWorkerId + " merged, cluster now: " + describeCluster());
        }
        catch (RemoteException e){
            log("bootstrap with " + targetWorkerId + " FAILED: " + e.getMessage() + " -> standard gossip");
        }
    }

    /**Executed by the scheduler thread, exchanges load information between workers */
    private void gossipRound() {
        //Refreshing my view to hand it out with an updated version
        // (otherwise if a node is idle it is uncorrectly detected as crashed)
        refresh();
        //Running failure detection not to consider probably crashed nodes
        detectPeerFailure();
        List<String> candidates = new ArrayList<>(peers.keySet());
        if (candidates.isEmpty()) {
            return;
        }
        String target = candidates.get(random.nextInt(candidates.size()));
        gossipExecutor.submit(() -> performGossipExchange(target));
    }

    /** Performs the real gossip exchange concurrently, avoiding freezing in case of RMI delay */
    private void performGossipExchange(String target) {
        try {
            WorkerRemote stub = peers.get(target);
            if (stub == null) {
                return; // evicted concurrently by another operation
            }
            GossipRemote gossipStub = (GossipRemote) stub;
            ClusterState outgoing = clusterState.snapshot();
            log("round -> " + target + " (sending " + outgoing.size() + " views)");
            //This blocking call was a problem for failure detection, now decoupled from scheduled gossip rounds
            ClusterState response = gossipStub.exchangeState(outgoing);
            clusterState.mergeAll(response, this.workerId, this.peers);
            log("round <- " + target + " merged, cluster now: " + describeCluster());
        } catch (RemoteException e) {
            log("round with " + target + " FAILED: " + e.getMessage() + " -- evicting peer");
            //TODO:other place where peers table is updated
            peers.remove(target);
            forgetPeer(target);
        } catch (RuntimeException e) {
            log("round with " + target + " raised " + e);
        }
    }

    /**Refreshes the local view with a new version */
    private synchronized void refresh() {
        versionCounter++;
        WorkerView current = clusterState.get(workerId);
        clusterState.merge(current.withLoad(localLoad, versionCounter));
    }

    /**Checks for all timestamps in clusterState and removes nodes that timed out */
    private void detectPeerFailure() {
        long now = System.currentTimeMillis();
        List<String> crashedNodes = new ArrayList<>();
        clusterState.allViews().forEach((view) -> {
            String id = view.getWorkerId();
            if(!id.equals(this.workerId)){
                long elapsed = now - view.getLastHeartbeat();
                if(elapsed > CRASHED_WORKER_TRESHOLD_MS){
                    crashedNodes.add(id);
                }
            }
        });
        //Nodes removed after to avoid race condition problems on view iteration
        for(String id : crashedNodes){
            System.out.printf("[%s] FAILURE DETECTION: Node %s has timed out, removed from peers.%n", workerId, id);
            peers.remove(id);
            forgetPeer(id);
        }
    }

    private String describeCluster() {
        return clusterState.allViews().stream()
                .map(v -> v.getWorkerId() + "(load=" + v.getLoadCount() + ",v=" + v.getVersion() + ")")
                .collect(Collectors.joining(" "));
    }

    private void log(String message) {
        System.out.printf("[%s] [gossip=%s] %s%n", LocalDateTime.now(), workerId, message);
    }
}
