package com.progetto.rmi;

import com.progetto.gossip.ClusterState;
import com.progetto.job.Job;
import com.progetto.job.JobResult;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Worker-to-worker RMI surface: gossip load exchange and job forwarding for
 * load balancing. Kept separate from WorkerRemote (the client-facing API,
 * which here also carries the dynamic-join membership methods) even though
 * a single Worker instance implements and exports both under the same
 * bound stub.
 */
public interface GossipRemote extends Remote {

    /** Push-pull: merges callerState locally, then returns this worker's own (now updated) state. */
    ClusterState exchangeState(ClusterState callerState) throws RemoteException;

    /**
     * Executes a job forwarded by another worker, using the same execution
     * path as a locally-submitted job, and returns its result synchronously
     * once done.
     */
    JobResult forwardJob(Job job) throws RemoteException;
}
