package com.progetto.worker;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.rmi.JobNotFoundException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Work stealing (stealJob), asynchronous result delivery (pushResult) and recovery-after-restart,
 * exercised directly through the public/package API — no RMI registry involved, since these are all
 * plain methods on Worker. Every test uses its own freshly generated worker id (never a shared one)
 * so PersistenceManager's per-id log file can't leak state between tests, and cleans that file up
 * afterwards.
 */
class WorkerStealingTest {

    private static final long JOB_TIMEOUT_MS = 5000;

    private final List<String> idsToClean = new ArrayList<>();

    private Worker newWorker() {
        String id = "steal-test-" + UUID.randomUUID();
        idsToClean.add(id);
        return new Worker(id);
    }

    @AfterEach
    void cleanUp() {
        idsToClean.forEach(id -> new File("worker_" + id + ".log").delete());
    }

    private JobStatus awaitTerminal(Worker worker, String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = worker.getStatus(jobId);
            if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
                return status;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("job " + jobId + " did not finish within " + JOB_TIMEOUT_MS + "ms");
    }

    private Job sleepJob(String origin, long millis) {
        return new Job(new Task("SLEEP", Map.of("millis", millis)), origin);
    }

    // ---------- stealJob ----------

    @Test
    void stealJobReturnsNullWhenQueueIsEmpty() throws Exception {
        Worker worker = newWorker();

        assertNull(worker.stealJob("thief"));
    }

    @Test
    void stealJobReturnsNullWhenOnlyOneJobIsQueued() throws Exception {
        // A single queued job is presumably already being worked on (or about to be); stealing it
        // would leave the victim with nothing while contributing no real rebalancing.
        Worker worker = newWorker();
        worker.forwardJob(sleepJob("elsewhere", 200));

        assertNull(worker.stealJob("thief"));
    }

    @Test
    void stealJobTakesFromTheTailLeavingTheHeadToExecuteNormally() throws Exception {
        Worker worker = newWorker();
        Job head = sleepJob("elsewhere", 300);
        Job tail = sleepJob("elsewhere", 300);
        worker.forwardJob(head);
        worker.forwardJob(tail);

        Job stolen = worker.stealJob("thief");

        assertEquals(tail.getJobId(), stolen.getJobId());
        // The head job is left behind and must still complete normally on this worker.
        assertEquals(JobStatus.COMPLETED, awaitTerminal(worker, head.getJobId()));
    }

    @Test
    void stealJobFlipsStatusToDelegatedWhenThisWorkerIsTheOrigin() throws Exception {
        // The stolen job's true owner (the one a client would poll) is this worker; DELEGATED here
        // is what makes checkDelegatedJobsTimeout watch it and fall back to local execution if the
        // thief never reports back. Both jobs must be built with this worker's own id as origin for
        // the flip to apply, so newWorker()'s random id has to be known up front.
        String id = "steal-test-" + UUID.randomUUID();
        idsToClean.add(id);
        Worker worker = new Worker(id);
        worker.forwardJob(sleepJob(id, 300));
        worker.forwardJob(sleepJob(id, 300));

        Job stolen = worker.stealJob("thief");

        assertEquals(JobStatus.DELEGATED, stolen.getStatus());
    }

    @Test
    void stealJobLeavesStatusUnchangedWhenThisWorkerIsNotTheOrigin() throws Exception {
        // If the worker being stolen from is only an intermediate holder (it received the job via
        // forwardJob/a previous steal), it has no timeout to arm on the origin's behalf: only the
        // true origin's own copy transitions through DELEGATED.
        Worker worker = newWorker();
        Job first = sleepJob("origin-elsewhere", 300);
        Job second = sleepJob("origin-elsewhere", 300);
        worker.forwardJob(first);
        worker.forwardJob(second);

        Job stolen = worker.stealJob("thief");

        assertNotEquals(JobStatus.DELEGATED, stolen.getStatus());
    }

    // ---------- pushResult ----------

    @Test
    void pushResultCompletesAJobAndRecordsItsOutput() throws Exception {
        Worker worker = newWorker();
        Job job = sleepJob("origin-elsewhere", 5000); // long enough to still be queued when pushed
        worker.forwardJob(job);
        JobResult pushed = new JobResult(job.getJobId(), "computed-elsewhere", true, null);

        worker.pushResult(pushed);

        assertEquals(JobStatus.COMPLETED, worker.getStatus(job.getJobId()));
        assertEquals("computed-elsewhere", worker.getResult(job.getJobId()).getOutput());
    }

    @Test
    void pushResultOnAnUnknownJobIdIsANoOp() throws Exception {
        Worker worker = newWorker();

        worker.pushResult(new JobResult("no-such-job", "x", true, null));

        // No exception, and no phantom job was created by the push.
        assertThrows(JobNotFoundException.class, () -> worker.getStatus("no-such-job"));
    }

    @Test
    void aSecondPushForAnAlreadyTerminalJobIsIgnored() throws Exception {
        // Duplicate push guard: at-least-once delivery from a peer (or, before the fix below, a
        // resurrected queue entry) must not let a later push silently overwrite a job's real result.
        Worker worker = newWorker();
        Job job = sleepJob("origin-elsewhere", 5000);
        worker.forwardJob(job);
        worker.pushResult(new JobResult(job.getJobId(), "first", true, null));

        worker.pushResult(new JobResult(job.getJobId(), "second-should-be-ignored", true, null));

        assertEquals("first", worker.getResult(job.getJobId()).getOutput());
    }

    @Test
    void aJobCompletedViaPushWhileStillQueuedIsNeverReExecuted() throws Exception {
        // Regression for a livelock: a DELEGATED-timeout false alarm (the fixed, load-independent
        // 10s window in checkDelegatedJobsTimeout) can requeue a job locally while the remote peer
        // is still legitimately executing it. When the real result later arrives via pushResult, a
        // stale copy of that same job could still be sitting in this worker's own queue — and
        // neither the executor nor work stealing checked status before touching a queue entry, so
        // it could be resurrected (a steal flips it back to DELEGATED, re-arming the timeout) or, as
        // reproduced live against a real two-node cluster, executed a second time for real.
        //
        // pushResult now purges any stale queue entry for the job it completes; processLoop and
        // stealJob additionally discard a terminal job if they ever encounter one anyway.
        Worker worker = newWorker();

        // Occupy the single executor thread so `target` is guaranteed to still be sitting in the
        // queue, not already running, when the push arrives.
        Job occupier = sleepJob("origin-elsewhere", 1500);
        worker.forwardJob(occupier);
        Thread.sleep(200); // let processLoop actually pick `occupier` up first

        Job target = sleepJob("origin-elsewhere", 50);
        worker.forwardJob(target);
        JobResult pushed = new JobResult(target.getJobId(), "pushed-result", true, null);
        worker.pushResult(pushed);

        assertEquals(JobStatus.COMPLETED, worker.getStatus(target.getJobId()));
        assertEquals("pushed-result", worker.getResult(target.getJobId()).getOutput());

        // Let the occupier finish and give the executor time to reach where `target` used to sit.
        // Without the fix, this is where it gets picked up and genuinely re-executed, silently
        // overwriting the pushed result with a freshly computed one.
        Thread.sleep(1800);

        assertEquals(JobStatus.COMPLETED, worker.getStatus(target.getJobId()));
        assertEquals("pushed-result", worker.getResult(target.getJobId()).getOutput(),
                "a job completed via push must never be re-executed just because a stale copy was still queued");
    }

    // ---------- recovery after restart ----------

    @Test
    void aJobStillRunningWhenTheWorkerCrashesIsRecoveredAndFinished() throws Exception {
        String id = "steal-test-" + UUID.randomUUID();
        idsToClean.add(id);
        new File("worker_" + id + ".log").delete();

        Worker first = new Worker(id);
        Job job = first.submitJob(new Task("SUM", Map.of("numbers", List.of(10, 20))));
        // Not awaited: this simulates a crash while the job (or its persisted UPDATE_JOB event) was
        // still mid-flight, the same scenario a real kill -9 leaves behind.

        // A second instance with the same id reads the same log file, exactly like a restarted
        // process would — no RMI, no new registry, just the same on-disk state.
        Worker second = new Worker(id);

        assertEquals(JobStatus.COMPLETED, awaitTerminal(second, job.getJobId()));
        assertEquals(30, second.getResult(job.getJobId()).getOutput());
    }

    @Test
    void aJobAlreadyCompletedBeforeTheCrashIsNeverReExecutedAfterRestart() throws Exception {
        // Regression: publishResult used to persist a job's UPDATE_JOB event BEFORE flipping its
        // status to COMPLETED/FAILED, so the WAL only ever recorded the job's *previous* status
        // (RUNNING). Every restart then saw every already-finished job as still in flight and
        // re-executed it from scratch — reproduced live: submit demo jobs, kill -9, restart, watch
        // them all run a second time.
        String id = "steal-test-" + UUID.randomUUID();
        idsToClean.add(id);
        new File("worker_" + id + ".log").delete();

        Worker first = new Worker(id);
        Job job = first.submitJob(new Task("SUM", Map.of("numbers", List.of(2, 3))));
        assertEquals(JobStatus.COMPLETED, awaitTerminal(first, job.getJobId()));
        assertEquals(5, first.getResult(job.getJobId()).getOutput());

        Worker second = new Worker(id); // same log file: simulates a crash + restart

        assertEquals(JobStatus.COMPLETED, second.getStatus(job.getJobId()));
        assertEquals(5, second.getResult(job.getJobId()).getOutput());

        // If the bug were present, the job would now be re-queued and briefly flip back through
        // PENDING/RUNNING as it gets executed a second time.
        Thread.sleep(300);

        assertEquals(JobStatus.COMPLETED, second.getStatus(job.getJobId()));
        assertEquals(5, second.getResult(job.getJobId()).getOutput());
    }
}
