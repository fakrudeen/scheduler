package com.fakrudeen.scheduler;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduling system worker
 * This expects master as the first argument.
 * Author: Fakrudeen Ali Ahmed
 * Date: 24 Nov 2017
 */
public class Worker {
    private static final Logger LOGGER = Logger.getLogger(Worker.class.getName());

    public static final String HOSTNAME_ENV_VARIABLE = "HOSTNAME";
    public static final int WORKER_DELAY_MS = 1000;
    public static final int WORKER_FAILURE_DELAY_MS = 10000;
    public static final int BUFFER_SIZE = 1024;
    public static final String PATH_PREFIX = "/scheduler/";
    public static final String PROTOCOL = "http://";
    public static final int HEARTBEAT_PERIOD_MS = 15000;
    public static final int RANDOMIZATION_MAX_INTERVAL_MS = 5000;
    private static Gson gson = new Gson();
    private static volatile boolean enabled;

    private static ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) throws IOException, InterruptedException {
        if(1 != args.length) {
            System.out.println("Worker needs exactly one argument for master host:port.");
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> enabled = false));
        String master = args[0];
        String workerId = System.getenv(HOSTNAME_ENV_VARIABLE);
        join(master, workerId);
        setupHeartBeat(master, workerId);
        enabled = true;
        while(enabled) {
            try {
                Task task = schedule(master, workerId);
                LOGGER.log(Level.INFO, "Task:"+task);
                if (null != task) {
                    doWork(task);
                    finish(master, workerId, task);
                }
                Thread.sleep(WORKER_DELAY_MS);
            }catch(Exception e) {
                LOGGER.log(Level.WARNING, e.getMessage());
                Thread.sleep(WORKER_FAILURE_DELAY_MS);
            }
        }

        leave(master, workerId);
    }

    private static void doWork(Task task) throws InterruptedException {
        Thread.sleep(task.getSleepTimeInSec()*1000);
    }

    private static void join(String master, String workerId) throws IOException {
        handleMethod(master, workerId, null, "join");
    }

    private static void leave(String master, String workerId) throws IOException {
        handleMethod(master, workerId, null, "leave");
    }

    private static void heartbeat(String master, String workerId) throws IOException {
        handleMethod(master, workerId, null, "heartbeat");
    }

    private static void finish(String master, String workerId, Task task) throws IOException {
        handleMethod(master, workerId, task, "finished");
    }

    private static Task schedule(String master, String workerId) throws IOException {
        String taskString = handleMethod(master, workerId, null, "schedule");
        try {
            return gson.fromJson(taskString, Task.class);
        }catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage());
        }
        return null;
    }

    /**
     * Handles master HTTP method call
     * @param master Master host
     * @param workerId - this worker
     * @param task task if any
     *@param method method to call  @return output data
     */
    private static String handleMethod(String master, String workerId, Task task, final String method) throws IOException {
        URL url;
        if(null == task) {
            url = new URL(PROTOCOL + master + PATH_PREFIX + method + "/" + workerId);
        } else {
            url = new URL(PROTOCOL + master + PATH_PREFIX + method + "/" + workerId + "/" + task.getTaskName());
        }
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if(HttpURLConnection.HTTP_OK != connection.getResponseCode()) {
            LOGGER.log(Level.WARNING, method+" failed.");
        }
        return readData(connection);
    }

    /**
     * Reads data from connection
     * @param connection master connection
     * @return Data as string
     */
    private static String readData(HttpURLConnection connection) throws IOException {
        InputStream stream = connection.getInputStream();
        char[] response = new char[BUFFER_SIZE];
        StringBuilder builder = new StringBuilder();
        try(InputStreamReader reader = new InputStreamReader(stream, Charset.forName("UTF-8"))) {
            while(true) {
                int read = reader.read(response);
                if(-1 == read) {
                    break;
                }
                builder.append(response, 0, read);
            }
        }
        return builder.toString();
    }

    /**
     * Setup heart beat background process
     * @param master Master host
     * @param workerId - this worker
     */
    private static void setupHeartBeat(String master, String workerId) {
        executorService.scheduleAtFixedRate(() -> {
            try {
                Thread.sleep(new Random().nextInt(RANDOMIZATION_MAX_INTERVAL_MS));
                heartbeat(master, workerId);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, e.getMessage());
            }
        }, HEARTBEAT_PERIOD_MS, HEARTBEAT_PERIOD_MS, TimeUnit.MILLISECONDS);
    }
}
