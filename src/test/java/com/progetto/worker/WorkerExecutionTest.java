package com.progetto.worker;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import com.progetto.rmi.JobNotCompletedException;
import com.progetto.rmi.JobNotFoundException;
import com.progetto.rmi.WorkerRemote;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task execution and membership bookkeeping on a single worker. A bare {@code new Worker(id)} is
 * not exported over RMI and has no peers, so every job runs locally — which is exactly the path
 * under test here, with no registry or sockets involved.
 */
class WorkerExecutionTest {

    private static final long JOB_TIMEOUT_MS = 5000;

    private Worker worker;

    @BeforeEach
    void setUp() {
        worker = new Worker("test-worker");
    }

    private JobResult runToCompletion(Task task) throws Exception {
        return awaitTerminal(worker.submitJob(task).getJobId());
    }

    private JobResult awaitTerminal(String jobId) throws Exception {
        long deadline = System.currentTimeMillis() + JOB_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = worker.getStatus(jobId);
            if (status == JobStatus.COMPLETED || status == JobStatus.FAILED) {
                return worker.getResult(jobId);
            }
            Thread.sleep(10);
        }
        throw new AssertionError("job " + jobId + " did not finish within " + JOB_TIMEOUT_MS + "ms");
    }

    // ---------- task execution ----------

    @Test
    void sumAddsThePayloadNumbers() throws Exception {
        JobResult result = runToCompletion(new Task("SUM", Map.of("numbers", List.of(1, 2, 3, 4, 5))));

        assertTrue(result.isSuccess(), result.getErrorMessage());
        assertEquals(15, result.getOutput());
    }

    @Test
    void sleepReportsHowLongItSlept() throws Exception {
        JobResult result = runToCompletion(new Task("SLEEP", Map.of("millis", 10)));

        assertTrue(result.isSuccess(), result.getErrorMessage());
        assertEquals("slept 10ms", result.getOutput());
    }

    @Test
    void matrixMultiplicationMultipliesTheMatrices() throws Exception {
        JobResult result = runToCompletion(new Task("MATRIX_MULT", Map.of(
                "a", List.of(List.of(1, 2), List.of(3, 4)),
                "b", List.of(List.of(5, 6), List.of(7, 8)))));

        assertTrue(result.isSuccess(), result.getErrorMessage());
        assertEquals(List.of(List.of(19, 22), List.of(43, 50)), result.getOutput());
    }

    @Test
    void nonSquareMatricesMultiplyByTheInnerDimension() throws Exception {
        JobResult result = runToCompletion(new Task("MATRIX_MULT", Map.of(
                "a", List.of(List.of(1, 2, 3)),
                "b", List.of(List.of(4), List.of(5), List.of(6)))));

        assertTrue(result.isSuccess(), result.getErrorMessage());
        assertEquals(List.of(List.of(32)), result.getOutput());
    }

    // ---------- failure paths ----------

    @Test
    void anUnknownTaskTypeFailsTheJobWithoutKillingTheExecutor() throws Exception {
        JobResult failed = runToCompletion(new Task("NOPE", Map.of()));

        assertFalse(failed.isSuccess());
        assertTrue(failed.getErrorMessage().contains("Unknown task type"), failed.getErrorMessage());

        // The executor is a single long-lived thread: one bad task must not take the worker down.
        JobResult next = runToCompletion(new Task("SUM", Map.of("numbers", List.of(1, 1))));
        assertTrue(next.isSuccess(), "the worker must keep executing jobs after a failed one");
        assertEquals(2, next.getOutput());
    }

    @Test
    void aMissingPayloadEntryFailsTheJob() throws Exception {
        JobResult result = runToCompletion(new Task("SUM", Map.of()));

        assertFalse(result.isSuccess());
        assertTrue(result.getErrorMessage().contains("numbers"), result.getErrorMessage());
    }

    @Test
    void mismatchedMatrixDimensionsFailTheJob() throws Exception {
        JobResult result = runToCompletion(new Task("MATRIX_MULT", Map.of(
                "a", List.of(List.of(1, 2, 3)),
                "b", List.of(List.of(1, 2)))));

        assertFalse(result.isSuccess());
        assertTrue(result.getErrorMessage().contains("dimension mismatch"), result.getErrorMessage());
    }

    @Test
    void askingAboutAnUnknownJobFails() {
        assertThrows(JobNotFoundException.class, () -> worker.getStatus("no-such-job"));
        assertThrows(JobNotFoundException.class, () -> worker.getResult("no-such-job"));
    }

    @Test
    void askingForAResultBeforeItIsReadyFails() throws Exception {
        Job job = worker.submitJob(new Task("SLEEP", Map.of("millis", 2000)));

        assertThrows(JobNotCompletedException.class, () -> worker.getResult(job.getJobId()));
    }

    // ---------- duplicate delivery ----------

    @Test
    void aJobDeliveredTwiceIsExecutedOnlyOnce() throws Exception {
        // Forwarding is at-least-once by construction, so the same job can legitimately arrive here
        // more than once. Re-running it would double the work and, for a task with side effects,
        // double those too.
        Job job = new Job(new Task("SLEEP", Map.of("millis", 400)));

        worker.forwardJob(job);
        awaitTerminal(job.getJobId());

        worker.forwardJob(job);

        // A re-run would flip the job back to RUNNING for a further 400ms. Sampling well inside
        // that window catches it: with the guard in place the status never leaves COMPLETED.
        long until = System.currentTimeMillis() + 200;
        while (System.currentTimeMillis() < until) {
            assertEquals(JobStatus.COMPLETED, worker.getStatus(job.getJobId()),
                    "a duplicate delivery must not re-run a job this worker already executed");
            Thread.sleep(10);
        }
    }

    @Test
    void identicalTasksAreStillTwoDistinctJobs() throws Exception {
        // The guard keys on the per-submission job id, never on the task contents: two clients
        // asking for the same computation must both get their work done.
        JobResult first = runToCompletion(new Task("SUM", Map.of("numbers", List.of(2, 3))));
        JobResult second = runToCompletion(new Task("SUM", Map.of("numbers", List.of(2, 3))));

        assertNotEquals(first.getJobId(), second.getJobId());
        assertEquals(5, first.getOutput());
        assertEquals(5, second.getOutput());
    }

    // ---------- membership ----------

    @Test
    void registerPeerReplacesAStaleStub() throws Exception {
        // Regression: a worker that restarts keeps its id but is exported on a fresh anonymous
        // port, so the stub we already hold is dead. Keeping it would break every later call.
        WorkerRemote first = new RecordingPeer();
        WorkerRemote second = new RecordingPeer();

        worker.registerPeer("peer", first);
        worker.registerPeer("peer", second);

        assertSame(second, worker.getKnownPeers().get("peer"));
    }

    @Test
    void registerPeerIgnoresTheWorkerItself() throws Exception {
        worker.registerPeer("test-worker", new RecordingPeer());

        assertTrue(worker.getKnownPeers().isEmpty(), "a worker must not end up as its own peer");
    }

    @Test
    void getKnownPeersHandsOutADefensiveCopy() throws Exception {
        worker.registerPeer("peer", new RecordingPeer());

        worker.getKnownPeers().clear();

        assertEquals(1, worker.getKnownPeers().size(), "callers must not be able to mutate our membership");
    }

    /** Distinct identity is all these tests need from a peer; nothing is ever invoked on it. */
    private static final class RecordingPeer implements WorkerRemote {
        @Override
        public Job submitJob(Task task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobStatus getStatus(String jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JobResult getResult(String jobId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerPeer(String peerId, WorkerRemote peerStub) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, WorkerRemote> getKnownPeers() {
            throw new UnsupportedOperationException();
        }
    }
}
