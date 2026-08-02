package com.progetto.gossip;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe, gossip-mergeable view of the cluster's load. Passed by value
 * over RMI during push-pull exchanges (Serializable), and merged with
 * whatever a worker already knows using last-writer-wins on the per-worker
 * version.
 */
public class ClusterState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ConcurrentHashMap<String, WorkerView> views = new ConcurrentHashMap<>();

    /** Applies incoming only if strictly newer than what's already known for that workerId. */
    public void merge(WorkerView incoming) {
        views.compute(incoming.getWorkerId(), (id, existing) ->
                (existing == null || incoming.getVersion() > existing.getVersion()) ? incoming : existing);
    }

    public void mergeAll(ClusterState incoming) {
        incoming.views.values().forEach(this::merge);
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
