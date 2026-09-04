package com.progetto.persistence;

import com.progetto.job.Job;
import com.progetto.job.JobResult;

import java.io.*;
import java.nio.file.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages state persistence for the Worker node using a Write-Ahead Logging (WAL) strategy.
 * This class ensures fault tolerance across node crashes by appending critical events 
 * (such as job creations, status transitions, and final results) to a local append-only log file.
 * Only jobs originated by this specific worker node are persisted.
 * Key mechanisms:
 *   Write-Ahead Logging: Events are immediately serialized into Base64 format and 
 *       flushed to disk to prevent data loss upon sudden crashes.
 *   State Recovery: On startup, the manager reloads the log file to reconstruct 
 *       the in-memory state of local jobs and completed results.
 */
public class PersistenceManager {

    //Wraps the content recovered from the file which is passed to the worker to handle
    public static class RecoveredState {
        public final Map<String, Job> jobs = new HashMap<>();
        public final Map<String, JobResult> results = new HashMap<>();
        
    }

    private final String filePath;
    private BufferedWriter writer;

    public PersistenceManager(String workerId) {
        this.filePath = "worker_" + workerId + ".log";

        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            this.writer = new BufferedWriter(new FileWriter(file, true));           
        } catch (IOException e){
            System.err.println("Error in log file inizialization: " + e.getMessage());
        }
    }

    public synchronized void appendEvent(String type, Object payload) {
        if(writer == null || payload == null) return;
        try {
            String serializedPayload = serialize(payload);
            writer.write(type + "|" + serializedPayload);
            writer.newLine();
            writer.flush();
        } catch (IOException e){
            System.err.println("Error while writing on disc: " + e.getMessage());
        }
    }

    /** Reads the log saved on disc, recovers all jobs and all jobs result and fills RecoveredState container */
    public RecoveredState recoverState() {
        RecoveredState state = new RecoveredState();
        Path path = Paths.get(filePath);
        if(!Files.exists(path)) return state;

        BufferedReader reader = null;
        try {
            reader = Files.newBufferedReader(path);
            String line;
            while ((line = reader.readLine()) != null) {
                //Continues are to bypass eventual inconsistencies of the log wo losing it entirely
                if (line.trim().isEmpty()) continue;

                String[] sections = line.split("\\|", 2);
                if (sections.length < 2) continue;

                String type = sections[0];
                Object data = deserialize(sections[1]);
                if (data == null) continue;

                if("UPDATE_JOB".equals(type) && data instanceof Job){
                    Job job = (Job) data;
                    state.jobs.put(job.getJobId(), job);
                } else if ("UPDATE_RESULT".equals(type) && data instanceof JobResult) {
                    JobResult result = (JobResult) data;
                    state.results.put(result.getJobId(), result);
                }
            }
        } catch (IOException e) {
            System.err.println("Error while reading state on the log: " + e.getMessage());
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    System.err.println("Error while closing the reader: " + e.getMessage());
                }
            }
        }
        return state;
    }

    public String getFilePath () {
        return this.filePath;
    }

    private String serialize (Object object) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)){
            objectStream.writeObject(object);
            return Base64.getEncoder().encodeToString(byteStream.toByteArray());
        } catch (IOException e) {
            return "";
        }
    }

    public static Object deserialize (String string) {
        try {
            byte[] bytes = Base64.getDecoder().decode(string);
            try (ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return objectStream.readObject();
            }
        } catch (Exception e) {
            return null;
        }
    }
}
