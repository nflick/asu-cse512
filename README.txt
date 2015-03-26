Prerequisites:
1.  Some version of JDK7, preferably OpenJDK7.
2.  Apache Maven 3.3
3.  Apache Spark 1.2.1 (If not using 1.2.1, see the note at the end of this readme)

To build the project:
1.  cd to the geospatial directory, which is under the directory that this readme is contained in.
    This contains the pom.xml file which Maven will use to build the project.

2.  run "mvn package" to compile and package the program into a JAR file. This file will
    be dropped in the target directory, underneath the geospatial directory. There may
    be several JAR files here, the relevant one is geospatial.jar.

To run the project:
1.  IMPORTANT: This project reads and writes files exactly according to the schema given
    in the project requirements document. When we were given test case files that did
    not match this schema, we elected to modify the test cases by adding ID columns and
    such so that they match the specified schema, rather than modifying our code which
    would make it not conform to the project requirements. Therefore, the input files 
    that you are testing must conform exactly to the project requirements. In particular,
    for the spatial join both inputs must have ID's and for the spatial range the first
    input must have IDs. For the spatial range, the output is simply an ID on each row
    of the output, for each rectangle that fits within the query file, unlike the test
    cases which have the x and y coordinates in the output. Verify that your test data
    matches the schema specified in the requirements document. ALSO: it's not clear
    from the document whether the columns are seperated by a comma alone "," or a comma
    and a space ", ". Our code assumes that columns are seperated by a comma alone,
    no whitespace, and no trailing newline on the last line. We have included the
    test cases that we have been using in the testcases directory under the directory
    that this readme is in. This directory contains the test cases, modified to match
    the schema of the project requirements document.

2.  cd to the target directory. This is located at geospatial/target relative to the
    directory that this readme is in.

3.  The program is invoked on the command line. The syntax is:
    java -jar geospatial.jar <MASTER> <SPARK_HOME> <OPERATION> <ARG 1> [ARG 2] <OUTPUT>
    where:
    MASTER is the address of the master node with the spark protocol specifier, like
        spark://X.X.X.X:7077. This doesn't seem to work if a hostname is provided instead
        of an IP address for some reason, so use an IP address.
    SPARK_HOME is the file system location where the Spark files are located on all
        of the machines.
    OPERATION is one of:
        "closest-points" (NOTE it is closest-points NOT closest-pair)
        "convex-hull"
        "range"
        "join-query"
        "union"
        "farthest-points" (NOTE it is farthest-points NOT farthest-pair)
    ARG 1 is the HDFS location of the first input file. This needs to be a fully qualified
        HDFS file location, like hdfs://X.X.X.X:54310/testcases/1.csv
    ARG 2 is needed for the operations that take two inputs: range and join-query. It is
        of the same format as ARG 1.
    OUTPUT is the fully qualified HDFS location where the output should be saved.

    A typical invokation on our system would this look like:
    java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark union hdfs://10.0.0.1:54310/testcases/PolygonUnionTestData.csv hdfs://10.0.0.1:54310/testout/union
    (to run the union operaton on the file in HDFS at testcases/PolygonUnionTestData.csv
    and write the output to HDFS at /testout/union)

POSSIBLE SPARK VERSION INCOMPATIBILITY:
We developed and tested against Spark 1.2.1. If you are not using Spark
1.2.1, it is likely that the client library will be mismatched and the program
will not work. You can probably solve this by modifying geospatial/pom.xml and
changing the lines that read

dependency>
	<groupId>org.apache.spark</groupId>
	<artifactId>spark-core_2.10</artifactId>
	<version>1.2.1</version>
</dependency>

Change the version to match the version of Spark that you are using, then recompile
the project with "mvn package". This should solve version incompatibility issues.