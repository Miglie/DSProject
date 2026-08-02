package com.progetto.gossip;

import java.io.Serializable;

/**
 * Gossip-exchanged snapshot of one worker's load. Instances are immutable:
 * any change (load, heartbeat) produces a new instance with a higher
 * version — that version is what ClusterState.merge() compares to decide
 * whether an incoming view is newer than what it already has.
 *
 * No host/port here: peer identity and connectivity are already handled by
 * the dynamic join protocol (Worker.peers holds live WorkerRemote stubs
 * keyed by workerId), so gossip only needs to carry load information.
 */
public final class WorkerView implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String workerId;
    private final int loadCount;
    private final long version;
    private final long lastHeartbeat;

    public WorkerView(String workerId, int loadCount, long version, long lastHeartbeat) {
        this.workerId = workerId;
        this.loadCount = loadCount;
        this.version = version;
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getWorkerId() {
        return workerId;
    }

    public int getLoadCount() {
        return loadCount;
    }

    public long getVersion() {
        return version;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    /** Returns a new view for the same worker with updated load/version/heartbeat. */
    public WorkerView withLoad(int newLoadCount, long newVersion) {
        return new WorkerView(workerId, newLoadCount, newVersion, System.currentTimeMillis());
    }

    @Override
    public String toString() {
        return "WorkerView{id='" + workerId + "', load=" + loadCount + ", v=" + version + '}';
    }
}
