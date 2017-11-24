package com.fakrudeen.scheduler;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.testng.Assert;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.testng.Assert.*;

/**
 * Author: Fakrudeen Ali Ahmed
 * Date: 24 Nov 2017
 *
 */
public class MongoTaskDBTest {
    @org.testng.annotations.Test
    public void testGetUnScheduledTasks() throws Exception {
        MongoTaskDB mongoTaskDB = new MongoTaskDB(new MockMongoClient());
        List<Task> unScheduledTasks = mongoTaskDB.getUnScheduledTasks();
        Assert.assertEquals(unScheduledTasks.size(), 4, "didn't find 4 unscheduled tasks.");
    }

    @org.testng.annotations.Test
    public void testGetRunningTasks() throws Exception {
        MongoTaskDB mongoTaskDB = new MongoTaskDB(new MockMongoClient());
        List<Task> runningTasks = mongoTaskDB.getRunningTasks();
        Assert.assertEquals(runningTasks.size(), 3, "didn't find 3 unscheduled tasks.");
    }

    @org.testng.annotations.Test
    public void testUpdateTask() throws Exception {
        MockMongoClient mongoClient = new MockMongoClient();
        mongoClient.mockMongoDatabase.updateCalled = false;
        mongoClient.mockMongoDatabase.status = ITaskDB.Status.killed.toString();
        MongoTaskDB mongoTaskDB = new MongoTaskDB(mongoClient);
        mongoTaskDB.updateTask("task1", ITaskDB.Status.killed);
        Assert.assertTrue(mongoClient.mockMongoDatabase.updateCalled, "Update method wasn't called");
    }

    @org.testng.annotations.Test
    public void testUpdateTaskAndHost() throws Exception {
        MockMongoClient mongoClient = new MockMongoClient();
        mongoClient.mockMongoDatabase.updateCalled = false;
        mongoClient.mockMongoDatabase.status = ITaskDB.Status.running.toString();
        MongoTaskDB mongoTaskDB = new MongoTaskDB(mongoClient);
        mongoTaskDB.updateTaskAndHost("task1", ITaskDB.Status.running, "host1");
        Assert.assertTrue(mongoClient.mockMongoDatabase.updateCalled, "Update method wasn't called");
    }

    public static class MockMongoClient extends MongoClient {
        MockMongoDatabase mockMongoDatabase = new MockMongoDatabase();
        @Override
        public MongoDatabase getDatabase(String databaseName) {
            return mockMongoDatabase;
        }
    }

    public static class MockMongoDatabase implements MongoDatabase {
        private boolean updateCalled;
        private String status;

        @Override
        public String getName() {
            return null;
        }

        @Override
        public CodecRegistry getCodecRegistry() {
            return null;
        }

        @Override
        public ReadPreference getReadPreference() {
            return null;
        }

        @Override
        public WriteConcern getWriteConcern() {
            return null;
        }

        @Override
        public ReadConcern getReadConcern() {
            return null;
        }

        @Override
        public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
            return null;
        }

        @Override
        public MongoDatabase withReadPreference(ReadPreference readPreference) {
            return null;
        }

        @Override
        public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
            return null;
        }

        @Override
        public MongoDatabase withReadConcern(ReadConcern readConcern) {
            return null;
        }

        @Override
        public MongoCollection<Document> getCollection(String s) {
            return new MongoCollection<Document>() {
                @Override
                public MongoNamespace getNamespace() {
                    return null;
                }

                @Override
                public Class<Document> getDocumentClass() {
                    return null;
                }

                @Override
                public CodecRegistry getCodecRegistry() {
                    return null;
                }

                @Override
                public ReadPreference getReadPreference() {
                    return null;
                }

                @Override
                public WriteConcern getWriteConcern() {
                    return null;
                }

                @Override
                public ReadConcern getReadConcern() {
                    return null;
                }

                @Override
                public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> aClass) {
                    return null;
                }

                @Override
                public MongoCollection<Document> withCodecRegistry(CodecRegistry codecRegistry) {
                    return null;
                }

                @Override
                public MongoCollection<Document> withReadPreference(ReadPreference readPreference) {
                    return null;
                }

                @Override
                public MongoCollection<Document> withWriteConcern(WriteConcern writeConcern) {
                    return null;
                }

                @Override
                public MongoCollection<Document> withReadConcern(ReadConcern readConcern) {
                    return null;
                }

                @Override
                public long count() {
                    return 0;
                }

                @Override
                public long count(Bson bson) {
                    return 0;
                }

                @Override
                public long count(Bson bson, CountOptions countOptions) {
                    return 0;
                }

                @Override
                public <TResult> DistinctIterable<TResult> distinct(String s, Class<TResult> aClass) {
                    return null;
                }

                @Override
                public <TResult> DistinctIterable<TResult> distinct(String s, Bson bson, Class<TResult> aClass) {
                    return null;
                }

                @Override
                public FindIterable<Document> find() {
                    return null;
                }

                @Override
                public <TResult> FindIterable<TResult> find(Class<TResult> aClass) {
                    return null;
                }

                @Override
                public FindIterable<Document> find(Bson bson) {
                    boolean isRunning = bson.toString().contains("running");
                    return new FindIterable<Document>() {
                        @Override
                        public FindIterable<Document> filter(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> limit(int i) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> skip(int i) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> maxTime(long l, TimeUnit timeUnit) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> maxAwaitTime(long l, TimeUnit timeUnit) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> modifiers(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> projection(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> sort(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> noCursorTimeout(boolean b) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> oplogReplay(boolean b) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> partial(boolean b) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> cursorType(CursorType cursorType) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> batchSize(int i) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> collation(Collation collation) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> comment(String s) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> hint(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> max(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> min(Bson bson) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> maxScan(long l) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> returnKey(boolean b) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> showRecordId(boolean b) {
                            return null;
                        }

                        @Override
                        public FindIterable<Document> snapshot(boolean b) {
                            return null;
                        }

                        @Override
                        public MongoCursor<Document> iterator() {
                            return new MongoCursor<Document>() {
                                int i = 0;
                                @Override
                                public void close() {

                                }

                                @Override
                                public boolean hasNext() {
                                    return (isRunning)?(i <= 2):(i <= 3);
                                }

                                @Override
                                public Document next() {
                                    ++i;
                                    HashMap<String, Object> map = new HashMap<>();
                                    map.put(MongoTaskDB.TASKNAME_FIELD, "task"+i);
                                    map.put(MongoTaskDB.SLEEPTIME_IN_SEC_FIELD, 60.);
                                    if(isRunning) {
                                        map.put(MongoTaskDB.HOST_FIELD, "host"+i);
                                    }
                                    return new Document(map);
                                }

                                @Override
                                public Document tryNext() {
                                    return null;
                                }

                                @Override
                                public ServerCursor getServerCursor() {
                                    return null;
                                }

                                @Override
                                public ServerAddress getServerAddress() {
                                    return null;
                                }
                            };
                        }

                        @Override
                        public void forEach(Consumer<? super Document> action) {

                        }

                        @Override
                        public Spliterator<Document> spliterator() {
                            return null;
                        }

                        @Override
                        public Document first() {
                            return null;
                        }

                        @Override
                        public <U> MongoIterable<U> map(Function<Document, U> function) {
                            return null;
                        }

                        @Override
                        public void forEach(Block<? super Document> block) {

                        }

                        @Override
                        public <A extends Collection<? super Document>> A into(A objects) {
                            return null;
                        }

                    };
                }

                @Override
                public <TResult> FindIterable<TResult> find(Bson bson, Class<TResult> aClass) {
                    return null;
                }

                @Override
                public AggregateIterable<Document> aggregate(List<? extends Bson> list) {
                    return null;
                }

                @Override
                public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> list, Class<TResult> aClass) {
                    return null;
                }

                @Override
                public MapReduceIterable<Document> mapReduce(String s, String s1) {
                    return null;
                }

                @Override
                public <TResult> MapReduceIterable<TResult> mapReduce(String s, String s1, Class<TResult> aClass) {
                    return null;
                }

                @Override
                public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> list) {
                    return null;
                }

                @Override
                public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends Document>> list, BulkWriteOptions bulkWriteOptions) {
                    return null;
                }

                @Override
                public void insertOne(Document document) {

                }

                @Override
                public void insertOne(Document document, InsertOneOptions insertOneOptions) {

                }

                @Override
                public void insertMany(List<? extends Document> list) {

                }

                @Override
                public void insertMany(List<? extends Document> list, InsertManyOptions insertManyOptions) {

                }

                @Override
                public DeleteResult deleteOne(Bson bson) {
                    return null;
                }

                @Override
                public DeleteResult deleteOne(Bson bson, DeleteOptions deleteOptions) {
                    return null;
                }

                @Override
                public DeleteResult deleteMany(Bson bson) {
                    return null;
                }

                @Override
                public DeleteResult deleteMany(Bson bson, DeleteOptions deleteOptions) {
                    return null;
                }

                @Override
                public UpdateResult replaceOne(Bson bson, Document document) {
                    return null;
                }

                @Override
                public UpdateResult replaceOne(Bson bson, Document document, UpdateOptions updateOptions) {
                    return null;
                }

                @Override
                public UpdateResult updateOne(Bson bson, Bson bson1) {
                    return null;
                }

                @Override
                public UpdateResult updateOne(Bson bson, Bson bson1, UpdateOptions updateOptions) {
                    return null;
                }

                @Override
                public UpdateResult updateMany(Bson bson, Bson bson1) {
                    return null;
                }

                @Override
                public UpdateResult updateMany(Bson bson, Bson bson1, UpdateOptions updateOptions) {
                    return null;
                }

                @Override
                public Document findOneAndDelete(Bson bson) {
                    return null;
                }

                @Override
                public Document findOneAndDelete(Bson bson, FindOneAndDeleteOptions findOneAndDeleteOptions) {
                    return null;
                }

                @Override
                public Document findOneAndReplace(Bson bson, Document document) {
                    return null;
                }

                @Override
                public Document findOneAndReplace(Bson bson, Document document, FindOneAndReplaceOptions findOneAndReplaceOptions) {
                    return null;
                }

                @Override
                public Document findOneAndUpdate(Bson bson, Bson bson1) {
                    Assert.assertTrue(bson.toString().contains("task1"));
                    Assert.assertTrue(bson1.toString().contains(status));
                    if(status.contains("running")) {
                        Assert.assertTrue(bson1.toString().contains("host1"));
                    }
                    updateCalled = true;
                    return null;
                }

                @Override
                public Document findOneAndUpdate(Bson bson, Bson bson1, FindOneAndUpdateOptions findOneAndUpdateOptions) {
                    return null;
                }

                @Override
                public void drop() {

                }

                @Override
                public String createIndex(Bson bson) {
                    return null;
                }

                @Override
                public String createIndex(Bson bson, IndexOptions indexOptions) {
                    return null;
                }

                @Override
                public List<String> createIndexes(List<IndexModel> list) {
                    return null;
                }

                @Override
                public ListIndexesIterable<Document> listIndexes() {
                    return null;
                }

                @Override
                public <TResult> ListIndexesIterable<TResult> listIndexes(Class<TResult> aClass) {
                    return null;
                }

                @Override
                public void dropIndex(String s) {

                }

                @Override
                public void dropIndex(Bson bson) {

                }

                @Override
                public void dropIndexes() {

                }

                @Override
                public void renameCollection(MongoNamespace mongoNamespace) {

                }

                @Override
                public void renameCollection(MongoNamespace mongoNamespace, RenameCollectionOptions renameCollectionOptions) {

                }
            };
        }

        @Override
        public <TDocument> MongoCollection<TDocument> getCollection(String s, Class<TDocument> aClass) {
            return null;
        }

        @Override
        public Document runCommand(Bson bson) {
            return null;
        }

        @Override
        public Document runCommand(Bson bson, ReadPreference readPreference) {
            return null;
        }

        @Override
        public <TResult> TResult runCommand(Bson bson, Class<TResult> aClass) {
            return null;
        }

        @Override
        public <TResult> TResult runCommand(Bson bson, ReadPreference readPreference, Class<TResult> aClass) {
            return null;
        }

        @Override
        public void drop() {

        }

        @Override
        public MongoIterable<String> listCollectionNames() {
            return null;
        }

        @Override
        public ListCollectionsIterable<Document> listCollections() {
            return null;
        }

        @Override
        public <TResult> ListCollectionsIterable<TResult> listCollections(Class<TResult> aClass) {
            return null;
        }

        @Override
        public void createCollection(String s) {

        }

        @Override
        public void createCollection(String s, CreateCollectionOptions createCollectionOptions) {

        }

        @Override
        public void createView(String s, String s1, List<? extends Bson> list) {

        }

        @Override
        public void createView(String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {

        }
    }

}