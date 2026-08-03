package com.progetto.rmi;

import com.progetto.gossip.ClusterState;
import com.progetto.job.Job;

import java.rmi.Remote;
import java.rmi.RemoteException;


public interface GossipRemote extends Remote {

    /** Push-pull: merges callerState locally, then returns this worker's own (now updated) state. */
    ClusterState exchangeState(ClusterState callerState) throws RemoteException;

    /**
     * Accepts a job forwarded by another worker and queues it on the same
     * execution path as a locally-submitted one.
     * Returns as soon as the job is queued rather than when it finishes
     */
    void forwardJob(Job job) throws RemoteException;
}
