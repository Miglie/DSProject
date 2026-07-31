package com.progetto.rmi;

public class JobNotFoundException extends Exception {

    private static final long serialVersionUID = 1L;

    public JobNotFoundException(String jobId) {
        super("No job found with id: " + jobId);
    }
}
