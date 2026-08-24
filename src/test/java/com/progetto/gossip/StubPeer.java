package com.progetto.gossip;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.rmi.GossipRemote;
import com.progetto.rmi.WorkerRemote;

import java.util.Map;

/**
 * Placeholder peer. These tests exercise membership and load bookkeeping, which only ever ask
 * whether a worker id is <em>present</em> in the peers map. The client-facing calls therefore fail
 * loudly — reaching them means a test is going further than intended.
 *
 * <p>The gossip calls are the exception: driving a round makes the service pick a target and
 * exchange with it on its I/O pool. Implementing GossipRemote as a no-op keeps that background
 * exchange inert (an empty ClusterState merges into nothing) instead of failing on a cast, which is
 * also what a real exported stub looks like — it implements every remote interface of the worker.
 */
class StubPeer implements WorkerRemote, GossipRemote {

    @Override
    public ClusterState exchangeState(String callerId, WorkerRemote callerStub, ClusterState callerState) {
        return new ClusterState();
    }

    @Override
    public void forwardJob(Job job) {
        // no-op: these tests never assert on forwarded work
    }

    @Override
    public Job submitJob(Task task) {
        throw new UnsupportedOperationException("StubPeer is not meant to be called");
    }

    @Override
    public JobStatus getStatus(String jobId) {
        throw new UnsupportedOperationException("StubPeer is not meant to be called");
    }

    @Override
    public JobResult getResult(String jobId) {
        throw new UnsupportedOperationException("StubPeer is not meant to be called");
    }

    @Override
    public void registerPeer(String peerId, WorkerRemote peerStub) {
        throw new UnsupportedOperationException("StubPeer is not meant to be called");
    }

    @Override
    public Map<String, WorkerRemote> getKnownPeers() {
        throw new UnsupportedOperationException("StubPeer is not meant to be called");
    }
}
