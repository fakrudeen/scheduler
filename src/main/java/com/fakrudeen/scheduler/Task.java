package com.fakrudeen.scheduler;

/**
 * Scheduler task
 */
public class Task {
    private String taskName;
    private int sleepTimeInSec;
    private String host;

    public Task() {

    }

    public Task(String taskName, int sleepTimeInSec, String host) {
        this.taskName = taskName;
        this.sleepTimeInSec = sleepTimeInSec;
        this.host = host;
    }

    public String getTaskName() {
        return taskName;
    }

    public int getSleepTimeInSec() {
        return sleepTimeInSec;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "{" +
                "taskName:'" + taskName + '\'' +
                ", sleepTimeInSec:" + sleepTimeInSec +
                ", getHost:" + host +
                '}';
    }

}
