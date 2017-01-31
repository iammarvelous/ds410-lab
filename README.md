# DS410: Lab assignments

## Lab 1: Pig

```
A= LOAD 'hdfs:/ds410/lab1/tweets.csv' USING PigStorage(',') AS (tweeter, text, retweet:int);
B= GROUP A BY tweeter;
C = FOREACH B GENERATE group, SUM(A.retweet);
DUMP C;
```

## Lab 2: Spark-shell basics & HDFS

### HDFS command introduction
```
hdfs dfs -ls /ds410/tweets/
hdfs dfs -lsr /ds410/
hdfs dfs -cat /ds410/tweets/nyc-twitter-data-2013.csv
hdfs dfs -put README.md /user/YourPennStateID
hdfs dfs -get /user/YourPennStateID localFile
```

### Example

```
import java.io.PrintWriter
import java.io.File
val data = sc.textFile("hdfs:/ds410/tweets/nyc-twitter-data-2013.csv")
val lines = data.map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
// starts your code here
//
//
//
// end of your code
val top10 = sortedCounts.take(10)
val writer = new PrintWriter(new File("output.txt"))
top10.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
writer.close()
```

### References
* [HDFS document](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html)
* [Scala regular expression tutorial](https://www.tutorialspoint.com/scala/scala_regular_expressions.htm)
