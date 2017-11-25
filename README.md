# scheduler
Distributed Task Scheduler

Design is at: https://docs.google.com/document/d/19EbbLcXb_eyjSauIWOdBsEXhDOrpnkNp-GoN3HOYelo/edit?usp=sharing

Docker containers:
1. MongoDB:
docker run -d  -p 27017:27017 -it mongo mongod
2. Master:
docker run  --net="host"  -it --rm master
3. Workers:
docker run  --net="host" -h worker1 -it --rm worker
docker run  --net="host" -h worker2 -it --rm worker