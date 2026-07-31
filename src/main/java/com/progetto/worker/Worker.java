package com.progetto.worker;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.rmi.JobNotCompletedException;
import com.progetto.rmi.JobNotFoundException;
import com.progetto.rmi.WorkerRemote;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker implements WorkerRemote {

    private static final long serialVersionUID = 1L;

    private final String workerId;
    private final BlockingQueue<Job> queue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JobResult> results = new ConcurrentHashMap<>();

    public Worker(String workerId) {
        this.workerId = workerId;
        Thread executorThread = new Thread(this::processLoop, "worker-executor-" + workerId);
        executorThread.setDaemon(true);
        executorThread.start();
    }

    @Override
    public Job submitJob(Task task) throws RemoteException {
        Job job = new Job(task);
        jobs.put(job.getJobId(), job);
        log(job, "SUBMITTED (queued)");
        try {
            queue.put(job);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrupted while queueing job " + job.getJobId(), e);
        }
        return job;
    }

    @Override
    public JobStatus getStatus(String jobId) throws RemoteException, JobNotFoundException {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job.getStatus();
    }

    @Override
    public JobResult getResult(String jobId) throws RemoteException, JobNotFoundException, JobNotCompletedException {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        JobResult result = results.get(jobId);
        if (result == null) {
            throw new JobNotCompletedException(jobId, job.getStatus());
        }
        return result;
    }

    private void processLoop() {
        while (true) {
            try {
                Job job = queue.take();
                executeJob(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void executeJob(Job job) {
        job.setStatus(JobStatus.RUNNING);
        log(job, "RUNNING");
        try {
            Object output = runTask(job.getTask());
            job.setStatus(JobStatus.COMPLETED);
            results.put(job.getJobId(), new JobResult(job.getJobId(), output, true, null));
            log(job, "COMPLETED -> output=" + output);
        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            results.put(job.getJobId(), new JobResult(job.getJobId(), null, false, e.getMessage()));
            log(job, "FAILED -> error=" + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Object runTask(Task task) throws Exception {
        switch (task.getType()) {
            case "SUM": {
                List<Integer> numbers = (List<Integer>) task.getPayload().get("numbers");
                if (numbers == null) {
                    throw new IllegalArgumentException("SUM task requires a 'numbers' payload entry");
                }
                int sum = numbers.stream().mapToInt(Integer::intValue).sum();
                return sum;
            }
            case "SLEEP": {
                Object millisObj = task.getPayload().get("millis");
                if (millisObj == null) {
                    throw new IllegalArgumentException("SLEEP task requires a 'millis' payload entry");
                }
                long millis = ((Number) millisObj).longValue();
                Thread.sleep(millis);
                return "slept " + millis + "ms";
            }
            case "MATRIX_MULT": {
                List<List<Integer>> a = (List<List<Integer>>) task.getPayload().get("a");
                List<List<Integer>> b = (List<List<Integer>>) task.getPayload().get("b");
                if (a == null || b == null) {
                    throw new IllegalArgumentException("MATRIX_MULT task requires 'a' and 'b' payload entries");
                }
                return multiply(a, b);
            }
            default:
                throw new IllegalArgumentException("Unknown task type: " + task.getType());
        }
    }

    private List<List<Integer>> multiply(List<List<Integer>> a, List<List<Integer>> b) {
        int rowsA = a.size();
        int colsA = a.get(0).size();
        int rowsB = b.size();
        int colsB = b.get(0).size();
        if (colsA != rowsB) {
            throw new IllegalArgumentException(
                    "Matrix dimension mismatch: a is " + rowsA + "x" + colsA + ", b is " + rowsB + "x" + colsB);
        }

        List<List<Integer>> result = new ArrayList<>();
        for (int i = 0; i < rowsA; i++) {
            List<Integer> row = new ArrayList<>();
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += a.get(i).get(k) * b.get(k).get(j);
                }
                row.add(sum);
            }
            result.add(row);
        }
        return result;
    }

    private void log(Job job, String message) {
        System.out.printf("[%s] [worker=%s] job=%s status=%s :: %s%n",
                LocalDateTime.now(), workerId, job.getJobId(), job.getStatus(), message);
    }

    public static void start(String workerId, int port) throws RemoteException {
        // Default RMI response timeout is effectively unbounded;
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "5000");

        Worker worker = new Worker(workerId);
        WorkerRemote stub = (WorkerRemote) UnicastRemoteObject.exportObject(worker, 0);

        Registry registry = LocateRegistry.createRegistry(port);
        String bindName = "worker/" + workerId;
        registry.rebind(bindName, stub);

        System.out.printf("Worker '%s' ready. Bound as '%s' on port %d%n", workerId, bindName, port);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: Worker <port> [workerId]");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        String workerId = args.length > 1 ? args[1] : "worker-1";

        try {
            start(workerId, port);
        } catch (RemoteException e) {
            System.err.println("Failed to start worker RMI server: " + e.getMessage());
            System.exit(1);
        }
    }
}
