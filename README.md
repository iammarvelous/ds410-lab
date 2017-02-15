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
# Lab 3: Hashtag count
See in demo.

# Lab 4: Triangle count

* NodeID: ID of threads
* NodeUserID: user ID of thread initiator
* CommentUserID: user ID of a person who posts one or more "reply post"(s) to the thread
* Count: the number of reply posts by the CommentUserID
* TermNodeID: The name of subforums in which the thread occurs.

```
import scala.util.Try
import java.io.PrintWriter
import java.io.File
val lines = sc.textFile("hdfs:/ds410/lab4/CSNNetwork.csv")
val item  = lines.map(line => line.split(","))
val string_item = item.filter( i => Try(i(1).toInt).isSuccess && Try(i(2).toInt).isSuccess)

val int_item = string_item.map( cs => (cs(1).toInt, cs(2).toInt)).filter( cs => cs._1 != cs._2)
val edge_increase = int_item.map(cs => (if (cs._1 < cs._2) (cs._1, cs._2); else (cs._2, cs._1))).distinct()
val edge_decrease = int_item.map(cs => (if (cs._1 < cs._2) (cs._2, cs._1); else (cs._1, cs._2))).distinct()
val two_edge = edge_increase.join(edge_decrease).map( cs => (cs._2, cs._1))
// ((larger, smaller), middle)
val extended_edge_decrease = edge_decrease.map(cs => (cs, 1))
val triangle = extended_edge_decrease.join(two_edge)
val num_triangle = triangle.count()

val nodes = string_item.map(cs => cs(1) +  "," + cs(2)).flatMap(_.split(",")).distinct()
val num_nodes = nodes.count()

val result = num_triangle / (num_nodes * (num_nodes - 1) * (num_nodes - 2) / 6.0).toDouble

print(result, num_nodes, num_triangle)
```

### References
* [HDFS document](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html)
* [Scala regular expression tutorial](https://www.tutorialspoint.com/scala/scala_regular_expressions.htm)
* [sbt documentation](http://www.scala-sbt.org/0.13/docs/index.html)
