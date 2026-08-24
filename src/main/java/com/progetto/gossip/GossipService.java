package com.progetto.gossip;

import com.progetto.rmi.GossipRemote;
import com.progetto.rmi.WorkerRemote;

import java.rmi.RemoteException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.LongSupplier;
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
    private static final long GOSSIP_INTERVAL_MS = GOSSIP_INTERVAL_SECONDS * 1000L;
    /** How much lower a peer's load must be before we forward instead of running locally. */
    private static final int LOAD_IMBALANCE_THRESHOLD = 2;
    /**Failure detection threshold */
    private static final int CRASHED_WORKER_THRESHOLD_MS = 6000;
    /**
     * A round arriving this much later than scheduled means the stall was on our side (the JVM was
     * frozen, descheduled or GC-bound), not the peers'. Heartbeats cannot be trusted across such a
     * gap, so failure detection is skipped for that round.
     */
    private static final long SCHEDULER_STALL_THRESHOLD_MS = 2 * GOSSIP_INTERVAL_MS;

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
    /** Set by Worker once the object is exported; handed to peers so they can call us back. */
    private volatile WorkerRemote selfStub;
    /** Wall clock of the previous gossip round, used to notice that we were the ones stalled. */
    private volatile long lastRoundAt;

    /**
     * Source of "now" for heartbeats and failure detection. Injectable so that tests can move time
     * forward instantly instead of sleeping past a six-second threshold for every case.
     */
    private final LongSupplier clock;

    public GossipService(String workerId, ConcurrentHashMap<String, WorkerRemote> peers) {
        this(workerId, peers, System::currentTimeMillis);
    }

    /** Test seam: same service, with time under the caller's control. */
    GossipService(String workerId, ConcurrentHashMap<String, WorkerRemote> peers, LongSupplier clock) {
        this.workerId = workerId;
        this.peers = peers;
        this.clock = clock;

        versionCounter++;
        clusterState.merge(new WorkerView(workerId, 0, versionCounter, clock.getAsLong()), clock.getAsLong());
        this.lastRoundAt = clock.getAsLong();

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

    /** Must be called before start(): peers need a stub to call back on. */
    public void setSelfStub(WorkerRemote selfStub) {
        this.selfStub = selfStub;
    }

    public void start() {
        lastRoundAt = clock.getAsLong();
        // Fixed *delay*, not fixed rate: gossip has no need for an absolute cadence, and a fixed
        // rate would keep a backlog of overdue rounds after any stall and then fire them all
        // back-to-back — half a dozen identical exchanges in a few milliseconds, achieving nothing.
        // The stall detection in gossipRound() still sees the same elapsed gap either way.
        scheduler.scheduleWithFixedDelay(this::gossipRound, GOSSIP_INTERVAL_SECONDS, GOSSIP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /** Called by Worker whenever a locally-executing job is enqueued (+1) or finishes (-1). */
    public synchronized void recordLocalLoadChange(int delta) {
        localLoad += delta;
        versionCounter++;
        WorkerView current = clusterState.get(workerId);
        long now = clock.getAsLong();
        clusterState.merge(current.withLoad(localLoad, versionCounter, now), now);
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
        adoptVersionFloor(callerState);
        clusterState.mergeAll(callerState, this.workerId, this.peers, clock.getAsLong());
        return clusterState.snapshot();
    }

    /**
     * Lifts the local version counter above whatever version of <em>us</em> the caller is holding.
     * After a restart the counter is back near zero while peers still gossip a high-versioned copy
     * of our old entry; without this, every update we produce would lose the last-writer-wins
     * comparison on the peers' side and our load would stay frozen at its pre-crash value forever.
     */
    private synchronized void adoptVersionFloor(ClusterState incoming) {
        WorkerView remoteSelf = incoming.get(workerId);
        if (remoteSelf != null && remoteSelf.getVersion() > versionCounter) {
            versionCounter = remoteSelf.getVersion();
        }
    }

    /** Drops a peer from the gossiped load view, e.g. after evicting it from membership on RPC failure. */
    public void forgetPeer(String peerId) {
        clusterState.remove(peerId);
        pendingForwards.remove(peerId);
    }

    /**Bootstrap first cluster view already up to date from seed node */
    public void bootstrap(GossipRemote seed, String targetWorkerId) {
        try{
            ClusterState response = seed.exchangeState(this.workerId, this.selfStub, this.clusterState.snapshot());
            clusterState.mergeAll(response, this.workerId, this.peers, clock.getAsLong());
            log("bootstrap round <- " + targetWorkerId + " merged, cluster now: " + describeCluster());
        }
        catch (RemoteException e){
            log("bootstrap with " + targetWorkerId + " FAILED: " + e.getMessage() + " -> standard gossip");
        }
    }

    /**Executed by the scheduler thread, exchanges load information between workers */
    //Package-private, not private: tests drive rounds explicitly instead of waiting on the scheduler.
    void gossipRound() {
        long now = clock.getAsLong();
        long sinceLastRound = now - lastRoundAt;
        lastRoundAt = now;

        //Refreshing my view to hand it out with an updated version
        // (otherwise if a node is idle it is uncorrectly detected as crashed)
        refresh();

        if (sinceLastRound > SCHEDULER_STALL_THRESHOLD_MS) {
            // We were frozen, not them: every heartbeat is stale by however long we were out, so
            // detection here would evict the whole cluster in one round and leave this node alone
            // for good. Forgive every timestamp instead and let the next rounds judge on fresh data.
            log("scheduler stalled for " + sinceLastRound + "ms -> skipping failure detection, "
                    + "granting all peers a fresh grace period");
            clusterState.touchAll(now);
        } else {
            //Running failure detection not to consider probably crashed nodes
            detectPeerFailure(now);
        }

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
            //Sending our own id and stub doubles as a membership announcement: it is how a node the
            //peer had evicted gets itself put back into that peer's membership.
            ClusterState response = gossipStub.exchangeState(this.workerId, this.selfStub, outgoing);
            adoptVersionFloor(response);
            clusterState.mergeAll(response, this.workerId, this.peers, clock.getAsLong());
            log("round <- " + target + " merged, cluster now: " + describeCluster());
        } catch (RemoteException e) {
            log("round with " + target + " FAILED: " + e.getMessage() + " -- evicting peer");
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
        long now = clock.getAsLong();
        clusterState.merge(current.withLoad(localLoad, versionCounter, now), now);
    }

    /**Checks for all timestamps in clusterState and removes nodes that timed out */
    private void detectPeerFailure(long now) {
        List<String> crashedNodes = new ArrayList<>();
        clusterState.allViews().forEach((view) -> {
            String id = view.getWorkerId();
            if(!id.equals(this.workerId)){
                long elapsed = now - view.getLastHeartbeat();
                if(elapsed > CRASHED_WORKER_THRESHOLD_MS){
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
