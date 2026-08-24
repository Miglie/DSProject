package com.progetto.gossip;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import com.progetto.rmi.WorkerRemote;

/**
 * Thread-safe, gossip-mergeable view of the cluster's load. Passed by value
 * over RMI during push-pull exchanges (Serializable), and merged with
 * whatever a worker already knows using last-writer-wins on the per-worker
 * version.
 */
public class ClusterState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConcurrentHashMap<String, WorkerView> views = new ConcurrentHashMap<>();

    /**
     * Applies incoming only if strictly newer than what's already known for that workerId.
     *
     * <p>The heartbeat is re-stamped with {@code now} — our local reading of when we last heard
     * something new about that worker — rather than trusting the timestamp travelling in the view,
     * which was taken on the sender's clock. Failure detection then compares local times only, so
     * it needs no clock synchronisation across the cluster. {@code now} is a parameter and not a
     * call to the system clock so that tests can drive the whole thing deterministically.
     */
    public void merge(WorkerView incoming, long now) {
        views.compute(incoming.getWorkerId(), (id, existing) ->
                (existing == null || incoming.getVersion() > existing.getVersion()) ? incoming.timestamp(now) : existing);
    }

    /**
     * Merges a full view sent by a remote peer into the local view. Employs a filter against the
     * list of known peers to avoid registering again into the clusterState precedently evicted
     * nodes.
     *
     * <p>A worker's own entry is never taken from a remote: this worker is the only authority on
     * its own load, and a peer can only ever hold an older copy of it. Adopting one would publish a
     * stale load and — after a restart, when the local version counter is back near zero while
     * peers still hold a high version — would pin the entry at that version forever, so no local
     * update could ever win the merge again. GossipService instead lifts its version counter above
     * whatever the remote knows (see adoptVersionFloor), which keeps versions monotonic across
     * restarts without ever importing foreign data about ourselves.
     */
    public void mergeAll(ClusterState incoming, String selfWorkerId, ConcurrentHashMap<String, WorkerRemote> peers, long now) {
        incoming.views.values().forEach(v -> {
            String id = v.getWorkerId();
            if(!id.equals(selfWorkerId) && peers.containsKey(id)){
                merge(v, now);
            }
        });
    }

    /**
     * Re-stamps every entry with the current time, granting the whole cluster a fresh grace period.
     * Used when the local scheduler has been stalled: every heartbeat then looks arbitrarily old
     * through no fault of the peers, and running failure detection on those timestamps would evict
     * the entire cluster at once.
     */
    public void touchAll(long now) {
        views.replaceAll((id, view) -> view.timestamp(now));
    }

    /** Defensive copy, safe to hand out over RMI or iterate without exposing the live map. */
    public ClusterState snapshot() {
        ClusterState copy = new ClusterState();
        copy.views.putAll(this.views);
        return copy;
    }

    public WorkerView get(String workerId) {
        return views.get(workerId);
    }

    /** Drops a worker entirely, e.g. once it's been evicted from membership after a failed RPC. */
    public void remove(String workerId) {
        views.remove(workerId);
    }

    public Collection<WorkerView> allViews() {
        return views.values();
    }

    public int size() {
        return views.size();
    }
}
