package com.progetto.client;

import com.progetto.job.Job;
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
 * Test client, in two flavours:
 *
 * <ul>
 *   <li><b>demo</b> (default) — one job per task type, to check the worker executes each correctly.</li>
 *   <li><b>stress</b> — a burst of N SLEEP jobs, to push one worker over the load-imbalance
 *       threshold and watch the forwarding/load balancing kick in.</li>
 * </ul>
 *
 * Both submit every job before polling any of them. That ordering is what makes the stress mode
 * work: polling in between would let the queue drain as fast as it fills, the load would never
 * build up, and no forwarding would ever trigger.
 */
public class Client {

    private static final long POLL_INTERVAL_MS = 300;
    private static final int DEFAULT_STRESS_JOBS = 8;
    private static final long DEFAULT_STRESS_SLEEP_MS = 3000;

    public static void main(String[] args) {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "5000");

        String host = args.length > 0 ? args[0] : "localhost";
        int port = intArg(args, 1, 1099);
        String workerId = args.length > 2 ? args[2] : "worker-1";

        boolean stress = args.length > 3;
        if (stress && !args[3].equalsIgnoreCase("stress")) {
            System.err.println("Unknown mode '" + args[3] + "'");
            System.err.println("Usage: Client [host] [port] [workerId] [stress [numJobs] [sleepMillis]]");
            return;
        }

        WorkerRemote worker;
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            worker = (WorkerRemote) registry.lookup("worker/" + workerId);
        } catch (RemoteException | NotBoundException e) {
            System.err.println("Failed to reach worker '" + workerId + "' at " + host + ":" + port
                    + ": " + e.getMessage());
            return;
        }

        List<Task> tasks = stress
                ? sleepBurst(intArg(args, 4, DEFAULT_STRESS_JOBS), longArg(args, 5, DEFAULT_STRESS_SLEEP_MS))
                : oneOfEachTaskType();

        List<String> jobIds = submitAll(worker, tasks);
        System.out.println("All " + jobIds.size() + " jobs submitted, polling for completion...");
        for (String jobId : jobIds) {
            awaitAndPrintResult(worker, jobId);
        }
    }

    private static List<Task> oneOfEachTaskType() {
        return List.of(
                new Task("SUM", Map.of("numbers", List.of(1, 2, 3, 4, 5))),
                new Task("SLEEP", Map.of("millis", 3000)),
                new Task("MATRIX_MULT", Map.of(
                        "a", List.of(List.of(1, 2), List.of(3, 4)),
                        "b", List.of(List.of(5, 6), List.of(7, 8)))));
    }

    private static List<Task> sleepBurst(int count, long sleepMillis) {
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tasks.add(new Task("SLEEP", Map.of("millis", sleepMillis)));
        }
        return tasks;
    }

    /** Submits every task, skipping (but reporting) any that the worker refuses. */
    private static List<String> submitAll(WorkerRemote worker, List<Task> tasks) {
        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            try {
                Job job = worker.submitJob(task);
                jobIds.add(job.getJobId());
                System.out.println("Submitted " + task.getType() + " job "
                        + (i + 1) + "/" + tasks.size() + ": " + job.getJobId());
            } catch (RemoteException e) {
                System.err.println("Failed to submit job " + (i + 1) + ": " + e.getMessage());
            }
        }
        return jobIds;
    }

    /** Polls one job to completion, reporting each status change, then prints its result. */
    private static void awaitAndPrintResult(WorkerRemote worker, String jobId) {
        try {
            JobStatus previous = null;
            while (true) {
                JobStatus status = worker.getStatus(jobId);
                if (status != previous) {
                    System.out.println("Job " + jobId + " status: " + status);
                    previous = status;
                }
                if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
                    break;
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
            System.out.println("Result for " + jobId + ": " + worker.getResult(jobId));
        } catch (RemoteException e) {
            System.err.println("RMI communication error polling " + jobId + ": " + e.getMessage());
        } catch (JobNotFoundException e) {
            System.err.println("Job lookup failed for " + jobId + ": " + e.getMessage());
        } catch (JobNotCompletedException e) {
            // The worker publishes a job's result before its terminal status, so the loop above
            // seeing COMPLETED/FAILED means the result is already there.
            System.err.println("Result not ready for " + jobId + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while polling " + jobId);
        }
    }

    private static int intArg(String[] args, int index, int fallback) {
        return args.length > index ? Integer.parseInt(args[index]) : fallback;
    }

    private static long longArg(String[] args, int index, long fallback) {
        return args.length > index ? Long.parseLong(args[index]) : fallback;
    }
}
