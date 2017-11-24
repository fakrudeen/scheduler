package com.fakrudeen.scheduler;

/**
 * Master interface of distributed task scheduler
 * Author: Fakrudeen Ali Ahmed
 * Date: 23 Nov 2017
 */
public interface IMaster {
    /**
     * A worker joins the cluster using this function
     * @param workerId worker identity, needs to be globally unique - could be an UUID or hostname.
     */
    public void join(String workerId);
    /**
     * A worker leaves the cluster using this function
     * @param workerId worker identity
     */
    public void leave(String workerId);
    /**
     * A worker periodically sends heartbeats using this function
     * @param workerId worker identity
     */
    public void heartBeat(String workerId);
    /**
     * A worker asks for work using this function
     * @param workerId worker identity
     * @return a task, null if nothing is available.
     */
    public Task schedule(String workerId);
    /**
     * A worker informs task completion using this function
     * @param workerId worker identity
     * @param task completed taskname
     */
    public void finished(String workerId, String task);
}
