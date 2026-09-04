package com.progetto.rmi;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;

import java.rmi.Remote;
import java.rmi.RemoteException;

import java.util.Map;

public interface WorkerRemote extends Remote {

    Job submitJob(Task task) throws RemoteException;
    JobStatus getStatus(String jobId) throws RemoteException, JobNotFoundException;
    JobResult getResult(String jobId) throws RemoteException, JobNotFoundException, JobNotCompletedException;

    //Methods for a worker to register when it connects to the net
    void registerPeer(String peerId, WorkerRemote peerStub) throws RemoteException;
    Map<String, WorkerRemote> getKnownPeers() throws RemoteException;

    //TODO:Queste due funzioni devono ancora essere implementate
    /** Enables a push approach to notify the origin about a JobResult */
    void pushResult(JobResult result) throws RemoteException;
}
