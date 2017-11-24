package com.fakrudeen.scheduler;

import org.testng.Assert;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchedulerTest {
    private static String workerId = "worker1";
    private static String taskName = "task1";

    @org.testng.annotations.Test
    public void testJoin() throws Exception {
        Scheduler scheduler = new Scheduler(new MockTaskDB());
        scheduler.join(workerId);
        Map<String, Long> workerToHeartBeatMap = getWorkerToHeartBeatMap(scheduler);
        Assert.assertTrue(workerToHeartBeatMap.containsKey(workerId), "Joined worker is missing.");
    }

    @org.testng.annotations.Test
    public void testLeave() throws Exception {
        Scheduler scheduler = new Scheduler(new MockTaskDB());
        Map<String, Long> workerToHeartBeatMap = getWorkerToHeartBeatMap(scheduler);
        workerToHeartBeatMap.put(workerId, System.currentTimeMillis());
        scheduler.leave(workerId);
        Assert.assertFalse(workerToHeartBeatMap.containsKey(workerId), "Worker is still part of cluster.");
    }

    @org.testng.annotations.Test
    public void testHeartBeat() throws Exception {
        Scheduler scheduler = new Scheduler(new MockTaskDB());
        Map<String, Long> workerToHeartBeatMap = getWorkerToHeartBeatMap(scheduler);
        long currentTimeMillis = System.currentTimeMillis();
        workerToHeartBeatMap.put(workerId, currentTimeMillis);
        Thread.sleep(1);
        scheduler.heartBeat(workerId);
        Assert.assertTrue(workerToHeartBeatMap.get(workerId) > currentTimeMillis, "Heartbeat wasn't updated.");
    }

    @org.testng.annotations.Test
    public void testSchedule() throws Exception {
        Scheduler scheduler = new Scheduler(new MockTaskDB());
        Map<String, String> workerToTaskMap = getWorkerToTaskMap(scheduler);
        scheduler.schedule(workerId);
        Assert.assertTrue(workerToTaskMap.containsKey(workerId), "Task map wasn't updated.");
    }

    @org.testng.annotations.Test
    public void testFinished() throws Exception {
        Scheduler scheduler = new Scheduler(new MockTaskDB());
        Map<String, String> workerToTaskMap = getWorkerToTaskMap(scheduler);
        workerToTaskMap.put(workerId, taskName);
        scheduler.finished(workerId, taskName);
        Assert.assertFalse(workerToTaskMap.containsKey(workerId), "Task map wasn't updated.");
    }

    private Map<String, Long> getWorkerToHeartBeatMap(Scheduler scheduler) throws NoSuchFieldException, IllegalAccessException {
        Field workerToHeartBeatMapField = Scheduler.class.getDeclaredField("workerToHeartBeatMap");
        workerToHeartBeatMapField.setAccessible(true);
        return (Map<String, Long>) workerToHeartBeatMapField.get(scheduler);
    }

    private Map<String, String> getWorkerToTaskMap(Scheduler scheduler) throws NoSuchFieldException, IllegalAccessException {
        Field workerToHeartBeatMapField = Scheduler.class.getDeclaredField("workerToTaskMap");
        workerToHeartBeatMapField.setAccessible(true);
        return (Map<String, String>) workerToHeartBeatMapField.get(scheduler);
    }


    private static class MockTaskDB implements ITaskDB {
        @Override
        public List<Task> getUnScheduledTasks() {
            ArrayList<Task> tasks = new ArrayList<>();
            tasks.add(new Task(taskName, 60, "host1"));
            return tasks;
        }

        @Override
        public List<Task> getRunningTasks() {
            return new ArrayList<>();
        }

        @Override
        public void updateTask(String task, Status status) {

        }

        @Override
        public void updateTaskAndHost(String task, Status status, String host) {

        }
    }
}