package com.progetto.persistence;

import com.progetto.job.Job;
import com.progetto.job.JobResult;
import com.progetto.job.JobStatus;
import com.progetto.job.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The write-ahead log in isolation: append/recover round-tripping and resilience to a malformed
 * file, with no Worker involved. PersistenceManager derives its file path from the worker id alone
 * ("worker_&lt;id&gt;.log" in the working directory, see getFilePath()), so every test here uses a
 * fresh UUID-suffixed id and deletes its file afterwards — sharing one id across tests would leak
 * WAL entries between them exactly as it once did in the Worker test suite.
 */
class PersistenceManagerTest {

    private final List<String> filesToClean = new ArrayList<>();

    private PersistenceManager newManager() {
        PersistenceManager manager = new PersistenceManager("pm-test-" + UUID.randomUUID());
        filesToClean.add(manager.getFilePath());
        return manager;
    }

    @AfterEach
    void cleanUp() {
        filesToClean.forEach(path -> new File(path).delete());
    }

    private Job aJob(String origin) {
        return new Job(new Task("SUM", Map.of("numbers", List.of(1, 2))), origin);
    }

    @Test
    void recoveringAFreshWorkerIdReturnsEmptyState() {
        PersistenceManager manager = newManager();

        PersistenceManager.RecoveredState state = manager.recoverState();

        assertTrue(state.jobs.isEmpty());
        assertTrue(state.results.isEmpty());
    }

    @Test
    void anAppendedJobIsRecoveredWithItsFields() {
        PersistenceManager manager = newManager();
        Job job = aJob("worker-x");
        job.setStatus(JobStatus.RUNNING);

        manager.appendEvent("UPDATE_JOB", job);
        PersistenceManager.RecoveredState state = manager.recoverState();

        Job recovered = state.jobs.get(job.getJobId());
        assertEquals(job.getJobId(), recovered.getJobId());
        assertEquals(JobStatus.RUNNING, recovered.getStatus());
        assertEquals("worker-x", recovered.getOriginWorkerId());
    }

    @Test
    void anAppendedResultIsRecoveredWithItsOutput() {
        PersistenceManager manager = newManager();
        JobResult result = new JobResult("job-1", 42, true, null);

        manager.appendEvent("UPDATE_RESULT", result);
        PersistenceManager.RecoveredState state = manager.recoverState();

        JobResult recovered = state.results.get("job-1");
        assertEquals(42, recovered.getOutput());
        assertTrue(recovered.isSuccess());
    }

    @Test
    void aLaterUpdateForTheSameJobIdOverwritesTheEarlierOneOnRecovery() {
        // The WAL is append-only and replayed in order: for a given jobId, whatever was written
        // last must win, mirroring how a real job's status only ever moves forward in time.
        PersistenceManager manager = newManager();
        Job job = aJob("worker-x");

        job.setStatus(JobStatus.RUNNING);
        manager.appendEvent("UPDATE_JOB", job);
        job.setStatus(JobStatus.COMPLETED);
        manager.appendEvent("UPDATE_JOB", job);

        PersistenceManager.RecoveredState state = manager.recoverState();

        assertEquals(JobStatus.COMPLETED, state.jobs.get(job.getJobId()).getStatus());
    }

    @Test
    void appendEventIgnoresANullPayloadInsteadOfWritingGarbage() {
        PersistenceManager manager = newManager();

        manager.appendEvent("UPDATE_JOB", null);

        assertTrue(manager.recoverState().jobs.isEmpty());
    }

    @Test
    void recoverStateSkipsAMalformedLineWithoutLosingTheRestOfTheLog() throws Exception {
        // Regression-shaped: a WAL is only useful if one corrupted line (a partial write cut off by
        // a crash, say) can't take the entire recovered state down with it.
        PersistenceManager manager = newManager();
        Job goodJob = aJob("worker-x");
        manager.appendEvent("UPDATE_JOB", goodJob);

        Files.writeString(Path.of(manager.getFilePath()), "UPDATE_JOB|not-valid-base64!!\n",
                StandardOpenOption.APPEND);

        PersistenceManager.RecoveredState state = manager.recoverState();

        assertEquals(1, state.jobs.size());
        assertEquals(goodJob.getJobId(), state.jobs.values().iterator().next().getJobId());
    }

    @Test
    void recoverStateSkipsBlankAndHeaderlessLines() throws Exception {
        PersistenceManager manager = newManager();
        Job goodJob = aJob("worker-x");
        manager.appendEvent("UPDATE_JOB", goodJob);

        Files.writeString(Path.of(manager.getFilePath()), "\nno-separator-here\n",
                StandardOpenOption.APPEND);

        PersistenceManager.RecoveredState state = manager.recoverState();

        assertEquals(1, state.jobs.size());
    }

    @Test
    void anUnrecognizedEventTypeIsIgnoredOnRecovery() {
        PersistenceManager manager = newManager();

        manager.appendEvent("SOMETHING_ELSE", aJob("worker-x"));

        assertTrue(manager.recoverState().jobs.isEmpty());
    }

    @Test
    void deserializeReturnsNullForInvalidInputInsteadOfThrowing() {
        assertNull(PersistenceManager.deserialize("not base64 at all!!"));
    }

    @Test
    void getFilePathNamesTheLogAfterTheWorkerId() {
        String id = "example-worker";
        PersistenceManager manager = new PersistenceManager(id);
        filesToClean.add(manager.getFilePath());

        assertEquals("worker_" + id + ".log", manager.getFilePath());
        assertTrue(new File(manager.getFilePath()).exists(), "the log file is created eagerly, not lazily on first write");
    }
}
