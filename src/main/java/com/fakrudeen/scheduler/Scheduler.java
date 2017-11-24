package com.fakrudeen.scheduler;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main Scheduler
 * Author: Fakrudeen Ali Ahmed
 * Date: 23 Nov 2017
 */
public class Scheduler implements IMaster {
    private static final Logger LOGGER = Logger.getLogger(Scheduler.class.getName());

    public static final int INITIAL_DELAY_MS = 5000;
    public static final int PERIOD_MS = 60000;
    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
    private ITaskDB taskDB;
    private Map<String, Long> workerToHeartBeatMap = new HashMap<>();
    private Map<String, String> workerToTaskMap = new HashMap<>();

    public Scheduler() {
        this(new MongoTaskDB());
    }

    public Scheduler(ITaskDB taskDB) {
        this.taskDB = taskDB;
        initializeMaps();
        executorService.scheduleAtFixedRate(this::handleDeadWorkers, INITIAL_DELAY_MS, PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * A worker joins the cluster using this function
     * @param workerId worker identity, needs to be globally unique - could be an UUID or hostname.
     */
    @Override
    public void join(String workerId) {
        LOGGER.log(Level.INFO, "Join worker:"+workerId);
        heartBeat(workerId); // heart beat the worker
    }

    /**
     * A worker leaves the cluster using this function
     * @param workerId worker identity
     */
    @Override
    public void leave(String workerId) {
        LOGGER.log(Level.INFO, "Leave worker:"+workerId);
        if(workerToHeartBeatMap.containsKey(workerId)) {
            workerToHeartBeatMap.remove(workerId);
        }
    }

    /**
     * A worker periodically sends heartbeats using this function
     * @param workerId worker identity
     */
    @Override
    public void heartBeat(String workerId) {
        LOGGER.log(Level.INFO, "Heart beat worker:"+workerId);
        workerToHeartBeatMap.put(workerId, System.currentTimeMillis());
    }

    /**
     * A worker asks for work using this function
     * @param workerId worker identity
     * @return a task, null if nothing is available.
     */
    @Override
    public Task schedule(String workerId) {
        if(!workerToHeartBeatMap.containsKey(workerId)) {
            join(workerId);
        } else {
            heartBeat(workerId); // heart beat the worker
        }
        if(workerToTaskMap.containsKey(workerId)) { //worker needs to finish the current job first!
            LOGGER.log(Level.WARNING, "Worker is requesting task without finishing the current one:"+workerId);
            return null;
        }
        List<Task> tasks = taskDB.getUnScheduledTasks();
        Task task = null;
        if(null != tasks && 0 != tasks.size()) {
            task = tasks.get(0);
            taskDB.updateTaskAndHost(task.getTaskName(), ITaskDB.Status.running, workerId);
            workerToTaskMap.put(workerId, task.getTaskName());
        }
        LOGGER.log(Level.INFO, "Scheduled for worker:"+workerId+" task:"+task);
        return task;
    }

    /**
     * A worker asks for work using this function
     * @param workerId worker identity
     * @return a task, null if nothing is available.
     */
    @Override
    public void finished(String workerId, String task) {
        LOGGER.log(Level.INFO, "Worker "+workerId+" completed task:"+task);
        taskDB.updateTask(task, ITaskDB.Status.success);
        workerToTaskMap.remove(workerId);
    }

    /**
     * Handles dead worker tasks. This enables the tasks to be rescheduled at other workers instead of waiting forever.
     */
    private void handleDeadWorkers() {
        List<String> workersToRemove = new ArrayList<>();
        for(String worker:workerToHeartBeatMap.keySet()) {
            if(System.currentTimeMillis() - workerToHeartBeatMap.get(worker) >  2*PERIOD_MS) {
                LOGGER.log(Level.WARNING, "Removing dead worker:"+worker);
                if(workerToTaskMap.containsKey(worker)) {
                    String taskName = workerToTaskMap.get(worker);
                    taskDB.updateTask(taskName, ITaskDB.Status.killed);
                    workerToTaskMap.remove(worker);
                }
                workersToRemove.add(worker);
            }
        }

        for(String worker:workersToRemove) { // this needs to be done here to avoid concurrent modification to iterating collection
            leave(worker);
        }
    }

    /**
     * Initializes scheduler maps from DB
     */
    private void initializeMaps() {
        List<Task> runningTasks = taskDB.getRunningTasks();
        for(Task task:runningTasks) {
            String worker = task.getHost();
            workerToTaskMap.put(worker, task.getTaskName());
            workerToHeartBeatMap.put(worker, System.currentTimeMillis());
        }
    }
}
