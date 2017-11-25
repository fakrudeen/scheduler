#
# Oracle Java 8 Dockerfile
#
# https://github.com/dockerfile/java
# https://github.com/dockerfile/java/tree/master/oracle-java8
#

# Pull base image.
FROM java

ADD Scheduler.jar .

# Define default command.
CMD ["java", "-cp", "Scheduler.jar", "com.fakrudeen.scheduler.Worker", "172.17.0.1:8000"]
