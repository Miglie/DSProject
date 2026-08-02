package com.progetto.worker;

import com.progetto.gossip.ClusterState;
import com.progetto.gossip.GossipService;
import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.rmi.GossipRemote;
import com.progetto.rmi.JobNotCompletedException;
import com.progetto.rmi.JobNotFoundException;
import com.progetto.rmi.WorkerRemote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements WorkerRemote, GossipRemote {

    private static final long serialVersionUID = 1L;

    /**
     * A forwarded job's forwardJob() call blocks until the receiving worker
     * finishes executing it, so this must stay comfortably under the RMI
     * response timeout configured in start() (15s) or the caller will see a
     * transport timeout instead of a real result.
     */
    private static final long FORWARD_WAIT_TIMEOUT_SECONDS = 12;

    private final String workerId;
    private final BlockingQueue<Job> queue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JobResult> results = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WorkerRemote> peers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<JobResult>> completions = new ConcurrentHashMap<>();
    private final GossipService gossipService;
    private final ExecutorService forwardingExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "worker-forward");
        t.setDaemon(true);
        return t;
    });

    private WorkerRemote selfStub; //stub of local worker to send to the other nodes

    public Worker(String workerId) {
        this.workerId = workerId;
        this.gossipService = new GossipService(workerId, peers);

        Thread executorThread = new Thread(this::processLoop, "worker-executor-" + workerId);
        executorThread.setDaemon(true);
        executorThread.start();
    }

    @Override
    public Job submitJob(Task task) throws RemoteException {
        Job job = new Job(task);
        jobs.put(job.getJobId(), job);

        String target = gossipService.decideForwardTarget();
        if (target != null) {
            job.setStatus(JobStatus.RUNNING);
            log(job, "FORWARDING to peer " + target + " (load balancing)");
            forwardingExecutor.submit(() -> forwardToPeer(job, target));
        } else {
            enqueueLocally(job);
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

    @Override
    public void registerPeer(String peerId, WorkerRemote peerStub) throws RemoteException{
        //Avoid registering itself
        if (peerId.equals(this.workerId)) return;
        // putIfAbsent instead of containsKey+put: the previous check-then-act was not atomic,
        // so two concurrent joins for the same peerId could both pass the check and double-log.
        if (peers.putIfAbsent(peerId, peerStub) == null) {
            //Debugging print
            networkLog(peerId, "added");
        }
    }

    /*TODO: Capire se questa funzione serve...al massimo per uscire gracefully */
    @Override
    public void unregisterPeer(String peerId) throws RemoteException {
        if (peers.remove(peerId) != null) {
            networkLog(peerId, "removed");
        }
    }

    @Override
    public Map<String, WorkerRemote> getKnownPeers() throws RemoteException {
        // Defensive copy: don't hand out the live map backing this worker's internal state.
        return new HashMap<>(this.peers);
    }

    @Override
    public ClusterState exchangeState(ClusterState callerState) throws RemoteException {
        return gossipService.exchangeState(callerState);
    }

    @Override
    public JobResult forwardJob(Job job) throws RemoteException {
        log(job, "RECEIVED forwarded job from peer");
        enqueueLocally(job);
        try {
            return completions.get(job.getJobId()).get(FORWARD_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrupted while executing forwarded job " + job.getJobId(), e);
        } catch (Exception e) {
            throw new RemoteException("Error executing forwarded job " + job.getJobId(), e);
        }
    }

    /** Enqueues a job for local execution — shared by directly-submitted and forwarded-in jobs. */
    private void enqueueLocally(Job job) throws RemoteException {
        jobs.putIfAbsent(job.getJobId(), job);
        completions.putIfAbsent(job.getJobId(), new CompletableFuture<>());
        gossipService.recordLocalLoadChange(1);
        log(job, "QUEUED locally");
        try {
            queue.put(job);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrupted while queueing job " + job.getJobId(), e);
        }
    }

    /** Sends a job to a peer and, on success, records the result as if this worker had run it. */
    private void forwardToPeer(Job job, String peerId) {
        try {
            WorkerRemote stub = peers.get(peerId);
            if (stub == null) {
                throw new RemoteException("Peer " + peerId + " is no longer known");
            }
            JobResult result = ((GossipRemote) stub).forwardJob(job);
            job.setStatus(result.isSuccess() ? JobStatus.COMPLETED : JobStatus.FAILED);
            results.put(job.getJobId(), result);
            log(job, "FORWARD to " + peerId + " completed -> output=" + result.getOutput());
        } catch (Exception e) {
            // A timeout means the peer is busy (its own queue hadn't drained in time), not dead —
            // evicting it here would be permanent (nothing currently re-adds a peer to `peers`),
            // so a single overloaded moment would exclude it from load balancing forever. Only
            // evict on failures that actually indicate unreachability (e.g. connection refused).
            if (isTimeout(e)) {
                log(job, "FORWARD to " + peerId + " TIMED OUT (peer busy), executing locally instead");
            } else {
                log(job, "FORWARD to " + peerId + " FAILED (" + e.getMessage() + "), evicting peer and executing locally instead");
                peers.remove(peerId);
                gossipService.forgetPeer(peerId);
            }
            try {
                enqueueLocally(job);
            } catch (RemoteException re) {
                job.setStatus(JobStatus.FAILED);
                results.put(job.getJobId(), new JobResult(job.getJobId(), null, false,
                        "Forward failed and local fallback failed: " + re.getMessage()));
                log(job, "FALLBACK FAILED: " + re.getMessage());
            }
        }
    }

    private static boolean isTimeout(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof java.util.concurrent.TimeoutException) {
                return true;
            }
        }
        return false;
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
        JobResult result;
        try {
            Object output = runTask(job.getTask());
            job.setStatus(JobStatus.COMPLETED);
            result = new JobResult(job.getJobId(), output, true, null);
            log(job, "COMPLETED -> output=" + output);
        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            result = new JobResult(job.getJobId(), null, false, e.getMessage());
            log(job, "FAILED -> error=" + e.getMessage());
        }
        results.put(job.getJobId(), result);
        gossipService.recordLocalLoadChange(-1);
        CompletableFuture<JobResult> future = completions.get(job.getJobId());
        if (future != null) {
            future.complete(result);
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

    private void networkLog(String peerId, String message){
        System.out.printf("[%s] [worker=%s] :: %s%n",
                LocalDateTime.now(), workerId, message + " " + peerId);
    }

    public static Worker start(String workerId, int port) throws RemoteException {
        // Default RMI response timeout is effectively unbounded. 15s (not Step 1's 5s) because
        // forwardJob() now blocks for the full duration of a forwarded job's execution — the
        // timeout must exceed the longest expected job runtime.
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "15000");

        Worker worker = new Worker(workerId);
        WorkerRemote stub = (WorkerRemote) UnicastRemoteObject.exportObject(worker, 0);
        worker.selfStub = stub; //selfStub inizialization

        Registry registry = LocateRegistry.createRegistry(port);
        String bindName = "worker/" + workerId;
        registry.rebind(bindName, stub);

        worker.gossipService.start();

        System.out.printf("Worker '%s' ready. Bound as '%s' on port %d%n", workerId, bindName, port);
        return worker;
    }

    public void joinNetwork(String targetHost, int targetPort, String targetWorkerId){
        try{
            //Getting stub of targetPeer from RMI registry of seed node
            networkLog(targetWorkerId, "connecting to");
            Registry registry = LocateRegistry.getRegistry(targetHost, targetPort);
            WorkerRemote targetPeer = (WorkerRemote) registry.lookup("worker/" + targetWorkerId);
            //Adding seed node to local map
            this.peers.put(targetWorkerId, targetPeer);
            //Getting peer list from seed node and updating local map
            Map<String, WorkerRemote> remotePeers = targetPeer.getKnownPeers();
            for(Map.Entry<String, WorkerRemote> entry : remotePeers.entrySet()){
                if(!entry.getKey().equals(workerId)){
                    this.peers.put(entry.getKey(), entry.getValue());
                }
            }
            //Contacting all remote peers acquired to perform handshake
            List<String> failedPeers = new ArrayList<>();
            for(Map.Entry<String, WorkerRemote> entry : this.peers.entrySet()){
                try{
                    entry.getValue().registerPeer(this.workerId, this.selfStub);
                    networkLog(entry.getKey(), "handshake completed with");
                } catch (RemoteException e){
                    System.err.println("Failed to register to remote worker " + entry.getKey() + ": " + e.getMessage());
                    failedPeers.add(entry.getKey());
                }
            }

            for(String deadPeer : failedPeers) {
                this.peers.remove(deadPeer);
            }
        }
        catch (NotBoundException e){
            System.err.println("No worker found with id " + targetWorkerId);
        }
        catch (RemoteException e){
            System.err.println("Failed to connect to seed node (" + targetWorkerId + "): " + e.getMessage());
        }
    }


    public static void main(String[] args) {
        if (args.length != 2 && args.length != 5) {
            System.err.println("Usage: Worker <port> <workerId> [seedNodeIP] [seedNodePort] [seedNodeId]");
            System.exit(1);
        }

        int port = parsePort(args[0]);
        String workerId = args[1];
        Worker worker = null;

        try {
            worker = start(workerId, port);
        } catch (RemoteException e) {
            System.err.println("Failed to start worker RMI server: " + e.getMessage());
            System.exit(1);
        }

        if (args.length > 2) {
            String targetHost = args[2];
            int targetPort = parsePort(args[3]);
            String targetId = args[4];
            worker.joinNetwork(targetHost, targetPort, targetId);
        }
    }

    private static int parsePort(String raw) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            System.err.println("Invalid port: '" + raw + "'");
            System.exit(1);
            throw e; // unreachable, keeps the compiler happy about port being definitely assigned
        }
    }
}
