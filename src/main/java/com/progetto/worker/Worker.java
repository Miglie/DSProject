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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Worker implements WorkerRemote, GossipRemote {

    /** How often the origin re-asks a peer for the result of a job it forwarded. */
    private static final long FORWARD_POLL_INTERVAL_MS = 200;

    /**
     * Consecutive failed polls before the origin gives up on a peer that had already accepted a
     * job. Tolerating a few keeps a momentary hiccup from triggering a duplicate local re-run of
     * a job the peer is still perfectly happily executing.
     */
    private static final int MAX_CONSECUTIVE_POLL_FAILURES = 3;

    private final String workerId;
    private final BlockingQueue<Job> queue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JobResult> results = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WorkerRemote> peers = new ConcurrentHashMap<>();
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

    /** A task is submitted by the client. Gossip service is queried to choose if forward to peer or execute locally.
     * The task is wrapped into a Job and then returned to the client.
     */
    @Override
    public Job submitJob(Task task) throws RemoteException {
        Job job = new Job(task);
        jobs.put(job.getJobId(), job);

        String target = gossipService.reserveForwardTarget();
        if (target != null) {
            job.setStatus(JobStatus.RUNNING);
            log(job, "FORWARDING to peer " + target + " (load balancing)");
            forwardingExecutor.submit(() -> forwardToPeer(job, target));
        } else {
            enqueueLocally(job);
        }
        return job;
    }

    /** Client queries the status of a job. */
    @Override
    public JobStatus getStatus(String jobId) throws RemoteException, JobNotFoundException {
        Job job = jobs.get(jobId);
        if (job == null) {
            throw new JobNotFoundException(jobId);
        }
        return job.getStatus();
    }

    /**  */
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
        // putIfAbsent instead of containsKey+put to guarantee atomicity,
        // so two concurrent joins for the same peerId could both pass the check and double-log.
        if (peers.putIfAbsent(peerId, peerStub) == null) {
            //Debugging print
            networkLog(peerId, "added");
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

    /** Called on a stub to forward a job. Job gets enqueued locally by the receiver.*/
    @Override
    public void forwardJob(Job job) throws RemoteException {
        log(job, "RECEIVED forwarded job from peer");
        enqueueLocally(job);
    }

    private void enqueueLocally(Job job) throws RemoteException {
        jobs.putIfAbsent(job.getJobId(), job);
        gossipService.recordLocalLoadChange(1);
        log(job, "QUEUED locally");
        try {
            queue.put(job);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RemoteException("Interrupted while queueing job " + job.getJobId(), e);
        }
    }

    /**
     * Hands a job to a peer, waits for the peer's result and records it as if this worker had run
     * it — the client only ever talks to the worker it submitted to.
     */
    private void forwardToPeer(Job job, String peerId) {
        try {
            WorkerRemote stub = peers.get(peerId);
            if (stub == null) {
                throw new RemoteException("Peer " + peerId + " is no longer known");
            }
            ((GossipRemote) stub).forwardJob(job);
            JobResult result = awaitRemoteResult(stub, job.getJobId(), peerId);
            publishResult(job, result);
            log(job, "FORWARD to " + peerId + " completed -> output=" + result.getOutput());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log(job, "FORWARD to " + peerId + " ABANDONED (worker shutting down)");
        } catch (Exception e) {
            log(job, "FORWARD to " + peerId + " FAILED (" + e.getMessage() + "), evicting peer and executing locally instead");
            peers.remove(peerId);
            gossipService.forgetPeer(peerId);
            try {
                enqueueLocally(job);
            } catch (RemoteException re) {
                publishResult(job, new JobResult(job.getJobId(), null, false,
                        "Forward failed and local fallback failed: " + re.getMessage()));
                log(job, "FALLBACK FAILED: " + re.getMessage());
            }
        } finally {
            gossipService.releaseForwardSlot(peerId);
        }
    }

  
    private JobResult awaitRemoteResult(WorkerRemote stub, String jobId, String peerId)
            throws RemoteException, InterruptedException {
        int consecutiveFailures = 0;
        while (true) {
            try {
                return stub.getResult(jobId);
            } catch (JobNotCompletedException e) {
                consecutiveFailures = 0;
            } catch (JobNotFoundException e) {
                // The peer accepted the job and then forgot it — it restarted, so nobody is
                // running it and the origin has to.
                throw new RemoteException("Peer " + peerId + " lost forwarded job " + jobId, e);
            } catch (RemoteException e) {
                if (++consecutiveFailures >= MAX_CONSECUTIVE_POLL_FAILURES) {
                    throw e;
                }
            }
            Thread.sleep(FORWARD_POLL_INTERVAL_MS);
        }
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
            result = new JobResult(job.getJobId(), runTask(job.getTask()), true, null);
        } catch (Exception e) {
            result = new JobResult(job.getJobId(), null, false, e.getMessage());
        }
        publishResult(job, result);
        log(job, result.isSuccess()
                ? "COMPLETED -> output=" + result.getOutput()
                : "FAILED -> error=" + result.getErrorMessage());

        gossipService.recordLocalLoadChange(-1);
    }

    private void publishResult(Job job, JobResult result) {
        results.put(job.getJobId(), result);
        job.setStatus(result.isSuccess() ? JobStatus.COMPLETED : JobStatus.FAILED);
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
        // Default RMI response timeout is effectively unbounded. No call blocks for the duration
        // of a job any more (forwardJob() only enqueues, and the origin polls for the result), so
        // every call is expected to return promptly and this only has to be generous enough to
        // ride out a loaded-but-alive peer.
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10000");

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
            //Contacting all remote peers acquired to perform handshake, dropping the unreachable ones
            Iterator<Map.Entry<String, WorkerRemote>> it = this.peers.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, WorkerRemote> entry = it.next();
                try{
                    entry.getValue().registerPeer(this.workerId, this.selfStub);
                    networkLog(entry.getKey(), "handshake completed with");
                } catch (RemoteException e){
                    System.err.println("Failed to register to remote worker " + entry.getKey() + ": " + e.getMessage());
                    it.remove();
                }
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

        try {
            Worker worker = start(args[1], parsePort(args[0]));
            if (args.length == 5) {
                worker.joinNetwork(args[2], parsePort(args[3]), args[4]);
            }
        } catch (RemoteException e) {
            System.err.println("Failed to start worker RMI server: " + e.getMessage());
            System.exit(1);
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
