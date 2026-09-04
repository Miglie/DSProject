package com.progetto.worker;

import com.progetto.gossip.ClusterState;
import com.progetto.gossip.GossipService;
import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.persistence.PersistenceManager;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Worker implements WorkerRemote, GossipRemote {

    private final String workerId;
    private final ConcurrentLinkedDeque<Job> queue = new ConcurrentLinkedDeque<>();
    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JobResult> results = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WorkerRemote> peers = new ConcurrentHashMap<>();
    /**
     * Job ids this worker has taken on for local execution. Guards against the same job being
     * queued here twice — a duplicate forwardJob delivery, say. It deliberately does NOT reuse the
     * `jobs` map: on the origin of a forwarded job that map already holds the job even though this
     * worker never queued it, so keying the guard on `jobs` would suppress exactly the local
     * fallback the forwarding path depends on. Job ids are per-submission UUIDs, so two identical
     * tasks are two distinct jobs and both still run.
     */
    private final Set<String> locallyAccepted = ConcurrentHashMap.newKeySet();
    private final GossipService gossipService;
    private final PersistenceManager persistenceManager;
    private final ExecutorService forwardingExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "worker-forward");
        t.setDaemon(true);
        return t;
    });
    //Creates a scheduled thread to execute work stealing
    private final ScheduledExecutorService stealerScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "work-stealer");
        t.setDaemon(true);
        return t;
    });
    //Controls the timeouts of the jobs which have been delegated to other nodes. If timeout gets surpassed a local
    //fallback is activated
    private final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "timeout-watchdog");
        t.setDaemon(true);
        return t;
    });
    private Random random = new Random();
    private WorkerRemote selfStub; //stub of local worker to send to the other nodes

    //Timeout when we try to poll a empty queue before retrying
    private static final long QUEUE_POLL_SLEEP_MS = 50L;

    //If the result of a remote job is not received before the timeout, we assume something bad has happened and
    //queue the task again
    //WARNING: Tuning this parameter in a good way is fundamental for system performances
    private static final long DELEGATED_JOB_TIMEOUT_MS = 10000L;

    public Worker(String workerId) {
        this.workerId = workerId;
        this.gossipService = new GossipService(workerId, peers);
        this.persistenceManager = new PersistenceManager(workerId);

        PersistenceManager.RecoveredState state = this.persistenceManager.recoverState();
        this.jobs.putAll(state.jobs);
        this.results.putAll(state.results);

        rebuildQueueAfterRecovery();

        Thread executorThread = new Thread(this::processLoop, "worker-executor-" + workerId);
        executorThread.setDaemon(true);
        executorThread.start();

        stealerScheduler.scheduleWithFixedDelay(this::attemptWorkSteal, 1, 1, TimeUnit.SECONDS);
        timeoutScheduler.scheduleWithFixedDelay(this::checkDelegatedJobsTimeout, 2, 2, TimeUnit.SECONDS);
    }

    /** A task is submitted by the client. Gossip service is queried to choose if forward to peer or execute locally.
     * The task is wrapped into a Job and then returned to the client.
     */
    @Override
    public Job submitJob(Task task) throws RemoteException {
        Job job = new Job(task, this.workerId);
        jobs.put(job.getJobId(), job);

        String target = gossipService.reserveForwardTarget();
        if (target != null) {
            job.setStatus(JobStatus.DELEGATED);
            persistenceManager.appendEvent("UPDATE_JOB", job);
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

    /** Register a new peer in both the peers view and the clusterState view */
    @Override
    public void registerPeer(String peerId, WorkerRemote peerStub) throws RemoteException{
        //Avoid registering itself
        if (peerId.equals(this.workerId)) return;
        // Unconditional put, not putIfAbsent: a worker that restarts keeps its id but is exported on
        // a fresh anonymous port, so the stub we already hold is dead. Keeping the old one would
        // make every call to that peer fail forever. put() is atomic and returns the displaced
        // value, so the logging below still fires exactly once per actual change.
        WorkerRemote previous = peers.put(peerId, peerStub);
        if (previous == null) {
            //Debugging print
            networkLog(peerId, "added");
        } else if (!previous.equals(peerStub)) {
            networkLog(peerId, "stub refreshed (peer restarted?) for");
        }
    }

    @Override
    public Map<String, WorkerRemote> getKnownPeers() throws RemoteException {
        // Defensive copy: don't hand out the live map backing this worker's internal state.
        return new HashMap<>(this.peers);
    }

    /** Called inside GossipService, exchanges state info between peers */
    @Override
    public ClusterState exchangeState(String callerId, WorkerRemote callerStub, ClusterState callerState)
            throws RemoteException {
        // Anti-entropy on membership. Failure detection can only remove peers; this is the only
        // path that adds one back, and it needs no extra protocol — a peer we evicted announces
        // itself simply by carrying on gossiping. Without it a node that merely stalled would be
        // gone from our view for good and the cluster would stay split forever.
        //This is useful when a worker stops and after some time restarts working on its own: not useful
        //insted when the crash needs to be resolved manually (CTRL C on terminal)
        registerPeer(callerId, callerStub);
        return gossipService.exchangeState(callerState);
    }

    /** Called on a stub to forward a job. Job gets enqueued locally by the receiver.*/
    @Override
    public void forwardJob(Job job) throws RemoteException {
        log(job, "RECEIVED forwarded job from peer");
        enqueueLocally(job);
    }

    /** Called to steal a job from an overloaded peer */
    @Override
    public synchronized Job stealJob(String stealerId) throws RemoteException {
        if (queue.size() <= 1) return null;

        Job stolenJob = queue.pollLast();
        if (stolenJob != null) {
            gossipService.recordLocalLoadChange(-1);
            locallyAccepted.remove(stolenJob.getJobId());
            //If a job is being stolen from the origin node, status is changed to delegated
            if (stolenJob.getOriginWorkerId() != null && stolenJob.getOriginWorkerId().equals(this.workerId)) {
                stolenJob.setStatus(JobStatus.DELEGATED);
            }
            log(stolenJob, "Stolen by peer " + stealerId);
        }

        return stolenJob;
    }

    @Override
    public void pushResult(JobResult result) throws RemoteException {
        Job job = jobs.get(result.getJobId());
        if (job != null) {
            synchronized (job) {
                if (job.getStatus() == JobStatus.COMPLETED || job.getStatus() == JobStatus.FAILED) {
                    log(job, "Duplicate push result received from remote peer (ignored).");
                    return;
                }
            }
            results.put(result.getJobId(), result);
            job.setStatus(result.isSuccess() ? JobStatus.COMPLETED : JobStatus.FAILED);
            persistenceManager.appendEvent("UPDATE_RESULT", result);
            persistenceManager.appendEvent("UPDATE_JOB", job);
            log(job, "Push result received. Output: " + result.getOutput());
        }
    }

    /**Enqueue a job for local execution, updating local state. */
    private void enqueueLocally(Job job) throws RemoteException {
        if (!locallyAccepted.add(job.getJobId())) {
            log(job, "SKIPPED (already accepted for local execution here)");
            return;
        }
        job.setStatus(JobStatus.PENDING);
        //Saved in the log only if originated in that worker
        if(job.getOriginWorkerId().equals(this.workerId)) persistenceManager.appendEvent("UPDATE_JOB", job);
        jobs.putIfAbsent(job.getJobId(), job);
        // Counted before the put, not after: the executor could otherwise take and finish the job
        // (recording -1) before the +1 landed, leaving the load permanently negative.
        gossipService.recordLocalLoadChange(1);
        queue.offerLast(job);
        log(job, "QUEUED locally");
    }

    /**
     * Hands a job to another peer which is less overloaded, then returns. Result is notify asinchronously
     * by the executor, which by the way is completely transparent to the origin.
     */
    private void forwardToPeer(Job job, String peerId) {
        try {
            WorkerRemote stub = peers.get(peerId);
            if (stub == null) {
                throw new RemoteException("Peer " + peerId + " is no longer known");
            }
            ((GossipRemote) stub).forwardJob(job);
            log(job, "FORWARDED to " + peerId + " (awating for results)");
        } catch (Exception e) {
            log(job, "FORWARD to " + peerId + " FAILED (" + e.getMessage() + "), evicting peer and executing locally instead");
            // Eviction here is not the last word: if the peer was only stalled it re-registers
            // itself on its next gossip round (see exchangeState). Re-running the job locally is
            // still at-least-once — a peer that comes back may have executed it too.
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

    private void attemptWorkSteal() {
        int currentLoad = queue.size();
        double averageLoad = gossipService.getAverageClusterLoad();
        if (currentLoad >= averageLoad) return;
        
        List<String> targets = gossipService.getHigherLoadPeers();
        if (targets.isEmpty()) return;

        String targetId = targets.get(random.nextInt(targets.size()));
        GossipRemote targetStub = (GossipRemote) peers.get(targetId);
        if (targetStub == null) return;
        
        try {
            Job stolenJob = targetStub.stealJob(this.workerId);
            if (stolenJob != null) {
                log(stolenJob, "Stolen from peer " + targetId);
                enqueueLocally(stolenJob);
            }
        } catch (RemoteException e) {
            System.err.printf("[%s] Error stealing job from peer %s: %s%n", workerId, targetId, e.getMessage());
            peers.remove(targetId);
            gossipService.forgetPeer(targetId);
        }
    }

    private void checkDelegatedJobsTimeout () {
        long now = System.currentTimeMillis();

        for (Job job : jobs.values()) {
            if (job.getStatus() == JobStatus.DELEGATED){
                synchronized(job) {
                    if(job.getStatus() == JobStatus.DELEGATED && (now - job.getLastUpdated() >= DELEGATED_JOB_TIMEOUT_MS)){
                        log(job, "Timeout expired (no push result received). Re-queueing locally");
                        try {
                            enqueueLocally(job);
                        } catch (Exception e) {
                            System.err.printf("[%s] Error during timeout fallback: " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    private void processLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                //The call is not blocking -> If the queue is empty we timeoute a little before retrying
                Job job = queue.pollFirst();
                if (job != null) executeJob(job);
                else Thread.sleep(QUEUE_POLL_SLEEP_MS);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            catch (Exception e) {
                System.err.printf("[%s] Error in processLoop: %s%n", workerId, e.getMessage());
            }
        }
    }

    private void executeJob(Job job) {
        job.setStatus(JobStatus.RUNNING);
        persistenceManager.appendEvent("UPDATE_JOB", job);
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

    /**Result of a completed job is added to the worker state.*/
    private void publishResult(Job job, JobResult result) {
        String originId = job.getOriginWorkerId();

        if(originId == null || originId.equals(this.workerId)){
            persistenceManager.appendEvent("UPDATE_RESULT", result);
            persistenceManager.appendEvent("UPDATE_JOB", job);
            results.put(job.getJobId(), result);
            job.setStatus(result.isSuccess() ? JobStatus.COMPLETED : JobStatus.FAILED);
        }
        else {
            WorkerRemote originStub = peers.get(originId);
            if(originStub != null){
                try{
                    originStub.pushResult(result);
                } catch (RemoteException e) {
                    log(job, "Failed to push result to origin node" + originId + ": " + e.getMessage());
                }
            }else {
                log(job, "Origin worker " + originId + " is not reachable for pushing result. Result saved locally as fallback.");
                results.put(job.getJobId(), result);
                job.setStatus(result.isSuccess() ? JobStatus.COMPLETED : JobStatus.FAILED);
            }
        }
    }

    /** Insert again in the queue after recovery only jobs still pending (status PENDING or DELEGATED) or interrupted
     *  while running with this worker as origin. A job which was delegated remotely is always requeued locally
     * after recovery, because we do not save information abput the route jobs follow*/
    private void rebuildQueueAfterRecovery() {
        for(Job job : jobs.values()) {
            if(!this.workerId.equals(job.getOriginWorkerId()) ||
                job.getStatus() == JobStatus.COMPLETED ||
                job.getStatus() == JobStatus.FAILED) continue;
            
            job.setStatus(JobStatus.PENDING);
            queue.offerLast(job);
            locallyAccepted.add(job.getJobId());
            gossipService.recordLocalLoadChange(1);
            log(job, "Recovered and requeued");
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

    //Setting up RMI, starting GossipService, creating Worker instance
    public static Worker start(String workerId, int port) throws RemoteException {
        // Default RMI response timeout is effectively unbounded. No call blocks for the duration
        // of a job any more (forwardJob() only enqueues, and the origin polls for the result), so
        // every call is expected to return promptly and this only has to be generous enough to
        // ride out a loaded-but-alive peer.
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "10000");

        Worker worker = new Worker(workerId);
        WorkerRemote stub = (WorkerRemote) UnicastRemoteObject.exportObject(worker, 0);
        worker.selfStub = stub; //selfStub initialization
        //Gossip needs it too: every exchange carries our stub so peers can (re-)register us.
        worker.gossipService.setSelfStub(stub);

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
                    gossipService.forgetPeer(entry.getKey());
                    it.remove();
                }
            }

            //Bootstrap to acquire the ClusterView of seed node and start with already updated inforamtion
            //Only now, after the handshake: the seed filters incoming views against its own peer
            //list, so bootstrapping first would have had it silently discard the half of the
            //exchange we push, and it would have learnt our load a full gossip round later.
            gossipService.bootstrap((GossipRemote) targetPeer, targetWorkerId);
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

//TODO: Cancellazione best-effort sul peer prima del fallback locale: restringerebbe la finestra
//      di doppia esecuzione fra nodi diversi. La deduplica locale (locallyAccepted) esiste gia,
//      ed e usata anche dai path di work stealing e di timeout dei job DELEGATED.
