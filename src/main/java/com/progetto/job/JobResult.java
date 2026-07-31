package com.progetto.job;

import java.io.Serializable;

/**
 * Outcome of a completed (or failed) job.
 */
public class JobResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final Object output;
    private final boolean success;
    private final String errorMessage;

    public JobResult(String jobId, Object output, boolean success, String errorMessage) {
        this.jobId = jobId;
        this.output = output;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public String getJobId() {
        return jobId;
    }

    public Object getOutput() {
        return output;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "JobResult{jobId='" + jobId + "', success=" + success
                + ", output=" + output
                + (errorMessage != null ? ", error='" + errorMessage + "'" : "")
                + '}';
    }
}
