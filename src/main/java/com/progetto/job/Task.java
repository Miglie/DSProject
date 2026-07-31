package com.progetto.job;

import java.io.Serializable;
import java.util.Map;

public class Task implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String type;
    private final Map<String, Object> payload;

    public Task(String type, Map<String, Object> payload) {
        this.type = type;
        this.payload = payload;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Task{type='" + type + "', payload=" + payload + '}';
    }
}
