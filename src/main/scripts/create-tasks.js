/**
 * Script to create tasks in Mongo DB - it needs to be run locally.
 * Author: Fakrudeen Ali Ahmed
 * Date: 24 Nov 2017
 */
var db = connect('127.0.0.1:27017/scheduler');
for (i = 0; i < 100; i++) {
    db.tasks.insert({'taskname':'task'+i, 'sleeptime': Math.floor(Math.random()*100),'status':'created'});
}