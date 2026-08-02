package com.progetto.client;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.rmi.JobNotCompletedException;
import com.progetto.rmi.JobNotFoundException;
import com.progetto.rmi.WorkerRemote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Load-balancing stress test: submits a burst of SLEEP jobs to a single
 * worker in a tight loop (no polling in between), so its local load rises
 * fast enough to reliably cross the forwarding threshold — useful to
 * observe FORWARDING/RECEIVED log lines without depending on the timing of
 * multiple separate Client processes.
 */
public class StressClient {

    private static final long POLL_INTERVAL_MS = 300;

    public static void main(String[] args) {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "5000");

        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 1099;
        String workerId = args.length > 2 ? args[2] : "worker-1";
        int numJobs = args.length > 3 ? Integer.parseInt(args[3]) : 8;
        long sleepMillis = args.length > 4 ? Long.parseLong(args[4]) : 3000;

        WorkerRemote worker;
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            worker = (WorkerRemote) registry.lookup("worker/" + workerId);
        } catch (RemoteException | NotBoundException e) {
            System.err.println("Failed to reach worker '" + workerId + "' at " + host + ":" + port + ": " + e.getMessage());
            return;
        }

        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < numJobs; i++) {
            Task task = new Task("SLEEP", Map.of("millis", sleepMillis));
            try {
                Job job = worker.submitJob(task);
                jobIds.add(job.getJobId());
                System.out.println("Submitted job " + (i + 1) + "/" + numJobs + ": " + job.getJobId());
            } catch (RemoteException e) {
                System.err.println("Failed to submit job " + (i + 1) + ": " + e.getMessage());
            }
        }

        System.out.println("All jobs submitted, polling for completion...");
        for (String jobId : jobIds) {
            awaitAndPrintResult(worker, jobId);
        }
    }

    private static void awaitAndPrintResult(WorkerRemote worker, String jobId) {
        try {
            JobStatus status;
            while (true) {
                status = worker.getStatus(jobId);
                if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
                    break;
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
            JobResult result = worker.getResult(jobId);
            System.out.println("Result for " + jobId + ": " + result);
        } catch (RemoteException e) {
            System.err.println("RMI communication error polling " + jobId + ": " + e.getMessage());
        } catch (JobNotFoundException e) {
            System.err.println("Job lookup failed for " + jobId + ": " + e.getMessage());
        } catch (JobNotCompletedException e) {
            System.err.println("Result not ready for " + jobId + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while polling " + jobId);
        }
    }
}
