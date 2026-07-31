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
import java.util.List;
import java.util.Map;

public class Client {

    private static final long POLL_INTERVAL_MS = 300;

    public static void main(String[] args) {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "5000");

        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 1099;
        String workerId = args.length > 2 ? args[2] : "worker-1";

        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            WorkerRemote worker = (WorkerRemote) registry.lookup("worker/" + workerId);

            Task sumTask = new Task("SUM", Map.of("numbers", List.of(1, 2, 3, 4, 5)));
            Task sleepTask = new Task("SLEEP", Map.of("millis", 3000));
            Task matrixTask = new Task("MATRIX_MULT", Map.of(
                    "a", List.of(List.of(1, 2), List.of(3, 4)),
                    "b", List.of(List.of(5, 6), List.of(7, 8))));

            Job sumJob = worker.submitJob(sumTask);
            System.out.println("Submitted SUM job: " + sumJob.getJobId());

            Job sleepJob = worker.submitJob(sleepTask);
            System.out.println("Submitted SLEEP job: " + sleepJob.getJobId());

            Job matrixJob = worker.submitJob(matrixTask);
            System.out.println("Submitted MATRIX_MULT job: " + matrixJob.getJobId());

            awaitAndPrintResult(worker, sumJob.getJobId());
            awaitAndPrintResult(worker, sleepJob.getJobId());
            awaitAndPrintResult(worker, matrixJob.getJobId());

        } catch (RemoteException e) {
            System.err.println("RMI communication error: " + e.getMessage());
        } catch (NotBoundException e) {
            System.err.println("No worker bound under that name: " + e.getMessage());
        } catch (JobNotFoundException e) {
            System.err.println("Job lookup failed: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Client interrupted while polling.");
        }
    }

    private static void awaitAndPrintResult(WorkerRemote worker, String jobId)
            throws RemoteException, JobNotFoundException, InterruptedException {
        JobStatus status;
        while (true) {
            status = worker.getStatus(jobId);
            System.out.println("Job " + jobId + " status: " + status);
            if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
                break;
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
        try {
            JobResult result = worker.getResult(jobId);
            System.out.println("Result for " + jobId + ": " + result);
        } catch (JobNotCompletedException e) {
            // Shouldn't happen given the polling loop above, but the API allows it.
            System.err.println("Result not ready: " + e.getMessage());
        }
    }
}
