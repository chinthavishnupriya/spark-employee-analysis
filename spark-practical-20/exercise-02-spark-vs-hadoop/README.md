# Exercise 2 — Spark vs Hadoop MapReduce

## Practical Execution

### Common Input

Input file:

spark-practical-20/exercise-02-spark-vs-hadoop/data/input.txt

Contents:

Spark is fast
Spark is distributed
Hadoop is distributed
Spark is powerful

## Spark Execution

Spark command:

val lines = sc.textFile("file:///home/vishnupriya/my-spark-app/spark-practical-20/exercise-02-spark-vs-hadoop/data/input.txt")

val words = lines.flatMap(_.split("\\s+"))

val counts = words.map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)

counts.collect().sortBy(_._1).foreach(println)

Spark output:

(distributed,2)
(fast,1)
(hadoop,1)
(is,4)
(powerful,1)
(spark,3)

## Hadoop MapReduce Execution

Hadoop WordCount JAR:

/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar

HDFS input:

/user/vishnupriya/exercise-02/input/input.txt

Hadoop command:

hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount \
/user/vishnupriya/exercise-02/input/input.txt \
/user/vishnupriya/exercise-02/output

Hadoop output:

Hadoop	1
Spark	3
distributed	2
fast	1
is	4
powerful	1

## Hadoop Job Verification

Job completed successfully.

Map input records: 4
Map output records: 12
Reduce output records: 6
Map tasks: 1
Reduce tasks: 1

## Practical Comparison

Both Spark and Hadoop MapReduce produced the same word-count results from the same input dataset.

Spark execution was performed using an RDD transformation pipeline with flatMap, map, and reduceByKey.

Hadoop execution was performed using the built-in MapReduce WordCount example on HDFS.

The practical execution demonstrated that both frameworks can perform distributed batch word counting, while their execution models and APIs are different.
