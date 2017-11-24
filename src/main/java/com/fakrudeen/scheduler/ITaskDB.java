package com.fakrudeen.scheduler;

import java.util.List;

/**
 * Task DB interface of distributed task scheduler
 * Author: Fakrudeen Ali Ahmed
 * Date: 23 Nov 2017
 */
public interface ITaskDB {
    /**
     * Returns all the unscheduled tasks from DB
     * @return list of unscheduled tasks
     */
    public List<Task> getUnScheduledTasks();

    /**
     * Returns all the running tasks from DB
     * @return list of running tasks
     */
    public List<Task> getRunningTasks();

    /**
     * Updates a task status in DB
     * @param task task name
     * @param status task status
     */
    public void updateTask(String task, Status status);

    /**
     * Updates a task status and corresponding host in DB
     * @param task task name
     * @param status task status
     * @param host host corresponding to the status, say running. If null, skips host update.
     */
    public void updateTaskAndHost(String task, Status status, String host);

    /**
     * Valid status strings in DB
     */
    public static enum Status {created, running, killed, success}
}
