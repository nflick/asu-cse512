#!/bin/bash
set -x

cd ~/Shared/project/geospatial/target
ls -l

~/hadoop/bin/hadoop fs -rm -R /testout
~/hadoop/bin/hadoop fs -mkdir /testout

echo ">>> RUNNING CLOSEST PAIR"
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark closest-points hdfs://10.0.0.1:54310/testcases/FarthestPairandClosestPairTestData.csv hdfs://10.0.0.1:54310/testout/closest
echo ">>> RESULTS:"
~/hadoop/bin/hadoop fs -cat /testout/closest/part-00000
read -p ">>> Press [ENTER] to continue..."

echo ">>> RUNNING FARTHEST PAIR"
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark farthest-points hdfs://10.0.0.1:54310/testcases/FarthestPairandClosestPairTestData.csv hdfs://10.0.0.1:54310/testout/farthest
echo ">>> RESULTS:"
~/hadoop/bin/hadoop fs -cat /testout/farthest/part-00000
read -p ">>> Press [ENTER] to continue..."

echo ">>> RUNNING CONVEX HULL"
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark convex-hull hdfs://10.0.0.1:54310/testcases/ConvexHullTestData.csv hdfs://10.0.0.1:54310/testout/convex
echo ">>> RESULTS:"
~/hadoop/bin/hadoop fs -cat /testout/convex/part-00000
read -p ">>> Press [ENTER] to continue..."

echo ">>> RUNNING GEOMETRIC UNION"
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark union hdfs://10.0.0.1:54310/testcases/PolygonUnionTestData.csv hdfs://10.0.0.1:54310/testout/union
echo ">>> RESULTS:"
~/hadoop/bin/hadoop fs -cat /testout/union/part-00000
read -p ">>> Press [ENTER] to continue..."

echo ">>> RUNNING SPATIAL RANGE"
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark range hdfs://10.0.0.1:54310/testcases/RangeQueryTestData1.csv hdfs://10.0.0.1:54310/testcases/RangeQueryTestData2.csv hdfs://10.0.0.1:54310/testout/range
echo ">>> RESULTS:"
~/hadoop/bin/hadoop fs -cat /testout/range/part-00000
read -p ">>> Press [ENTER] to continue..."

echo ">>> RUNNING SPATIAL JOIN"
java -jar geospatial.jar spark://10.0.0.1:7077 /home/nathan/spark join-query hdfs://10.0.0.1:54310/testcases/JoinQueryTestData1.csv hdfs://10.0.0.1:54310/testcases/JoinQueryTestData2.csv hdfs://10.0.0.1:54310/testout/join
echo ">>> RESULTS:"
~/hadoop/bin/hadoop fs -cat /testout/join/part-00000
read -p ">>> Press [ENTER] to continue..."
