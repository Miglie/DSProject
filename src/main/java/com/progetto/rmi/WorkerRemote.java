package com.progetto.rmi;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface WorkerRemote extends Remote {

    Job submitJob(Task task) throws RemoteException;
    JobStatus getStatus(String jobId) throws RemoteException, JobNotFoundException;
    JobResult getResult(String jobId) throws RemoteException, JobNotFoundException, JobNotCompletedException;
}
