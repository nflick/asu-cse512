# ASU CSE 512 Course Project

Since the project documentation doesn't specify either a specific function signature or command line calling conventions that our code needs to have, I put a simple command line interface together for now. We can change it as we get more specific direction later on.

Let's put all of the code that applies to more than one function in the common class. It currently holds code for parsing point objects from strings and for reading and writing to/from HDFS. It also contains the main function which implements the CLI. To add another function to this CLI, you just need to add another case to the if/else block at the end of the main function, and check whether the command is the command you're implementing.

I still have not tested the closest points code on Spark, as I'm trying to sort out an error with some Akka configuration files not being included correctly. The current build system (see pom.xml) is set up to merge all of the dependencies together with our code into one final jar file, but it seems something is not happening correctly there as it throws an error about having no configuration value for akka.version. I will push a new version when I solve it.

I've added JTS to the maven dependencies. You'll have to follow the procedure in the hadoop/spark integration guide from the course documents to get maven to pull in all of the dependencies.

### How to run
```bash
java -jar geospatial.jar
Usage: java -jar geospatial.jar <MASTER> <SPARK_HOME> <COMMAND> <ARG 1> [ARG 2] <OUTPUT>
    where COMMAND is one of { closest-points, farthest-points ... }
# For instance
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark closest-points hdfs://10.0.0.1:54310/samples/points.txt hdfs://10.0.0.1:54310/samples/closest.txt
```

### Current Error
The error that is currently preventing it from working is somewhat confusing because it occurs in Spark's code and the stack trace contains no code of our own, only java JRE, Scala, Akka, and Spark framework code. However it always fails on the reduce job in the closest points procedure. The error.txt file in the root of this repository contains the output that is given when the job is run. If anyone has any ideas, let me know.
