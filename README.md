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

# Lab 5: Kmeans Step

```
import scala.util.Try
import java.io.PrintWriter
import java.io.File
import Array._

def initialCluster(nb_cluster:Int, nb_feature:Int) : Array[(Int, Array[Double])] = {
    var clusters = ofDim[(Int, Array[Double])](nb_cluster)
    for (i <- 0 to nb_cluster-1) {
        clusters(i) = (i, Array.fill(nb_feature){scala.util.Random.nextDouble()} )
    }
    return clusters
}
def Distance(a:Array[Double], b:Array[Double]) : Double = {
    assert(a.length == b.length, "Distance(): features dim does not match.")
    var dist = 0.0
    for (i <- 0 to a.length-1) {
        dist = dist + math.pow(a(i) - b(i), 2)
    }
    return math.sqrt(dist)
}

// def step(Array[(Int, Array[Double])]) : Array[(Int, Array[Double])] = {}

val nb_cluster = 3
val nb_feature = 4
val lines = sc.textFile("/ds410/lab5/iris.data")
//val clusters = sc.broadcast(initialCluster(3, 4))
val clusters = sc.broadcast(Array((0,Array(5.1,3.5,1.4,0.2)), (1,Array(4.9,3.0,1.4,0.2)), (2,Array(4.7,3.2,1.3,0.2)) ))
val samples  = lines.map(line => line.split(",").slice(0,4).map(_.toDouble)).zipWithIndex().map(sample => (sample._2, sample._1))

// one update:

// Complete this line: 
// Expected output structure: (sampleID, (clusterID, Distance(sample, cluster))
// val dist = 


val labels = dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1))
// (sampleID, clusterID)

var  new_clusters = ofDim[(Int, Array[Double])](nb_cluster)
for (i <- 0 to nb_cluster-1) {
    val sample_in_cluster = samples.join(labels.filter(i==_._2))
    val total_number = sample_in_cluster.count
    if (total_number != 0) {
        var tmp = sample_in_cluster.map(sample => sample._2._1).reduce((a, b) => a.zip(b).map{ case (x, y) => x + y })
        tmp = tmp.map( a => a/total_number.toDouble)
        new_clusters(i) = (i, tmp)
    }
    else {
        new_clusters(i) = (i, samples.takeSample(false, 1)(0)._2)
    }
}
```
# Lab 6: Kmeans

```
import scala.util.Try
import java.io.PrintWriter
import java.io.File

class Kmeans (val k:Int, val f:Int) extends java.io.Serializable{
    val nb_cluster:Int = k
    val nb_feature:Int = f
    var centers:Array[(Int, List[Double])] = _

    def initialize(samples:org.apache.spark.rdd.RDD[(Long, Array[Double])]) : Array[(Int, List[Double])] = {
        val tmp = samples.takeSample(false, nb_cluster).map(c => c._2)
        val tmp1 = (0 to (nb_cluster-1)).toArray
        val tmp2 = tmp1.zip(tmp)
        val clusters = tmp2.map(c => (c._1, c._2.toList))
        return clusters
    }

    def Distance(a:Array[Double], b:List[Double]) : Double = {
        assert(a.length == b.length, "Distance(): features dim does not match.")
        var dist = 0.0
        for (i <- 0 to a.length-1) {
            dist = dist + math.pow(a(i) - b(i), 2)
        }
        return math.sqrt(dist)
    }

    def step(c:Array[(Int, List[Double])], samples:org.apache.spark.rdd.RDD[(Long, Array[Double])]) : Array[(Int, List[Double])] = {
            val clusters = sc.broadcast(c)
            val dist = samples.flatMap{ case(sampleID, sample) => clusters.value.map{
                case (clusterID, cluster) => (sampleID, (clusterID, Distance(sample, cluster)))
                }
            }
            val labels = dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1))
            var  new_clusters = Array.ofDim[(Int, Array[Double])](nb_cluster)
            for (i <- 0 to nb_cluster-1) {
                val sample_in_cluster = samples.join(labels.filter(i==_._2))
                val total_number = sample_in_cluster.count
                if (total_number != 0) {
                    var tmp = sample_in_cluster.map(sample => sample._2._1).reduce((a, b) => a.zip(b).map{ case (x, y) => x + y })
                    tmp = tmp.map( a => a/total_number.toDouble)
                    new_clusters(i) = (i, tmp)
                }
                else {
                    new_clusters(i) = (i, samples.takeSample(false, 1)(0)._2)
                }
            }
            val new_clusters_list = new_clusters.map(s => (s._1, s._2.toList))
            return new_clusters_list
    }

    def run(samples:org.apache.spark.rdd.RDD[(Long, Array[Double])], max_iter:Int) : Unit = {
        var i:Int = 0
        val t0 = System.nanoTime()
        centers = initialize(samples)
        while(i < max_iter) {
            centers = step(centers, samples)
            i += 1
        }
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1-t0)/10e9 + "s.")
    }
}

val lines = sc.textFile("/ds410/lab5/iris.data")
val samples  = lines.map(line => line.split(",").slice(0,4).map(_.toDouble)).zipWithIndex().map(sample => (sample._2, sample._1))
val k = new Kmeans(3, 4)
k.run(samples, 100)
val centers = k.centers
```


### References
* [HDFS document](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-common/FileSystemShell.html)
* [Scala regular expression tutorial](https://www.tutorialspoint.com/scala/scala_regular_expressions.htm)
* [sbt documentation](http://www.scala-sbt.org/0.13/docs/index.html)
