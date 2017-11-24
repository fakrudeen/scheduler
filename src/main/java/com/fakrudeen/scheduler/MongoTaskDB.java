package com.fakrudeen.scheduler;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MongoDB based Task DB implementation
 * Author: Fakrudeen Ali Ahmed
 * Date: 23 Nov 2017
 */
public class MongoTaskDB implements ITaskDB {
    private static final Logger LOGGER = Logger.getLogger(MongoTaskDB.class.getName());

    public static final String DB_CONNECTION_STRING = "mongodb://localhost:27017";
    public static final String DATABASE_NAME = "scheduler";
    public static final String COLLECTION_NAME = "tasks";
    public static final String STATUS_FIELD = "status";
    public static final String TASKNAME_FIELD = "taskname";
    public static final String SLEEPTIME_IN_SEC_FIELD = "sleeptime";
    public static final String HOST_FIELD = "host";
    public static final String IN_OPERATOR = "$in";
    public static final String UPDATE_OPERATOR = "$set";

    private MongoClient mongoClient;

    public MongoTaskDB() {
        this(new MongoClient(new MongoClientURI(DB_CONNECTION_STRING)));
    }

    public MongoTaskDB(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    /**
     * Returns all the unscheduled tasks from DB
     * @return list of unscheduled tasks
     */
    public List<Task> getUnScheduledTasks() {
        ArrayList<String> statusList = new ArrayList<>();
        statusList.add(Status.created.toString());
        statusList.add(Status.killed.toString());
        return getTasks(statusList);
    }

    /**
     * Returns all the running tasks from DB
     * @return list of running tasks
     */
    public List<Task> getRunningTasks() {
        ArrayList<String> statusList = new ArrayList<>();
        statusList.add(Status.running.toString());
        return getTasks(statusList);
    }

    private List<Task> getTasks(List<String> statusList) {
        try {
            List<Task> tasks = new ArrayList<>();
            MongoCollection<Document> taskDocuments = getTaskDocuments();
            BasicDBObject inQuery = new BasicDBObject();
            inQuery.put(STATUS_FIELD, new BasicDBObject(IN_OPERATOR, statusList));
            for (Document taskDocument : taskDocuments.find(inQuery)) {
                tasks.add(new Task(taskDocument.getString(TASKNAME_FIELD), taskDocument.getDouble(SLEEPTIME_IN_SEC_FIELD).intValue(),  taskDocument.containsKey(HOST_FIELD)?taskDocument.getString(HOST_FIELD):null));
            }
            return tasks;
        }catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return null;
        }
    }

    /**
     * Updates a task status in DB
     * @param task task name
     * @param status task status
     */
    public void updateTask(String task, Status status) {
        updateTaskAndHost(task, status, null);
    }

    /**
     * Updates a task status and corresponding host in DB
     * @param task task name
     * @param status task status
     * @param host host corresponding to the status, say running. If null, skips host update.
     */
    public void updateTaskAndHost(String task, Status status, String host) {
        MongoCollection<Document> taskDocuments = getTaskDocuments();
        BasicDBObject updateFields = new BasicDBObject();
        updateFields.append(STATUS_FIELD, status.toString());
        if(null != host) {
            updateFields.append(HOST_FIELD, host);
        }
        BasicDBObject taskQuery = new BasicDBObject();
        taskQuery.put(TASKNAME_FIELD, task);
        taskDocuments.findOneAndUpdate(taskQuery, new BasicDBObject(UPDATE_OPERATOR, updateFields));
    }

    /**
     * Get all task documents from Mongo DB
     * @return task documents
     */
    private MongoCollection<Document> getTaskDocuments() {
        MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
        return database.getCollection(COLLECTION_NAME);
    }
}
