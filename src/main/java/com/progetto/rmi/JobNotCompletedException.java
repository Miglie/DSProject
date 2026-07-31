package com.progetto.rmi;

import com.progetto.job.JobStatus;

public class JobNotCompletedException extends Exception {

    private static final long serialVersionUID = 1L;

    public JobNotCompletedException(String jobId, JobStatus currentStatus) {
        super("Job " + jobId + " is not completed yet (current status: " + currentStatus + ")");
    }
}
