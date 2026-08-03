package com.progetto.job;

import java.io.Serializable;
import java.util.UUID;


public class Job implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final Task task;
    private volatile JobStatus status;
    private volatile long lastUpdated;

    public Job(Task task) {
        this.jobId = UUID.randomUUID().toString();
        this.task = task;
        this.status = JobStatus.PENDING;
        this.lastUpdated = System.currentTimeMillis();
    }

    public String getJobId() {
        return jobId;
    }

    public Task getTask() {
        return task;
    }

    public JobStatus getStatus() {
        return status;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
        this.lastUpdated = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "Job{jobId='" + jobId + "', status=" + status + ", task=" + task + '}';
    }
}
