import java.io.PrintWriter
import java.io.File
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions
import scala.io.Source
import scala.util.Try

object Demo {
    	// Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "demo"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
		val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
		val sc = new SparkContext(conf)

	    // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Parse Lines, Filter and Extract Tokens
		val lines = sc.textFile("hdfs:/ds410/lab4/CSNNetwork.csv")
		val item  = lines.map(line => line.split(","))
		val string_item = item.filter( i => Try(i(1).toInt).isSuccess && Try(i(2).toInt).isSuccess)

		// Convert to Vertex Pair
		val int_item = string_item.map( cs => (cs(1).toInt, cs(2).toInt)).filter( cs => cs._1 != cs._2)

		// Permute Edge Ordering
		val edge_increase = int_item.map(cs => (if (cs._1 < cs._2) (cs._1, cs._2); else (cs._2, cs._1))).distinct()
		val edge_decrease = int_item.map(cs => (if (cs._1 < cs._2) (cs._2, cs._1); else (cs._1, cs._2))).distinct()

		// Formulate Triangles
		val two_edge = edge_increase.join(edge_decrease).map( cs => (cs._2, cs._1))
		val extended_edge_decrease = edge_decrease.map(cs => (cs, 1))
		val triangle = extended_edge_decrease.join(two_edge)

		// Generate Triangle Tuples
		val triTuple = triangle.map(x=>(x._1._1,x._1._2,x._2._2))
		val triCount = triTuple.flatMap(t => List(t._1, t._2, t._3)).groupBy(identity).mapValues(_.size)

		// Compute Per Node Neighbor Counts
		val inc_group = edge_increase.groupByKey.mapValues(_.toList)
		val dec_group = edge_decrease.groupByKey.mapValues(_.toList)
		val nodeCount = inc_group.union(dec_group).groupByKey.mapValues(_.flatMap(x=>x).toList.length)

		// Compute Network Cluster Coefficient
		val results = triCount.fullOuterJoin(nodeCount).mapValues(x => if (x._1 != None) (x._1.get, (x._1.get / (x._2.get * (x._2.get-1) / 2.0).toDouble)) else (0, 0)).sortByKey().collect()

        // Generate Output File
        val writer = new PrintWriter(new File("output.txt"))
        results.foreach(x => writer.write(x._1 + "\t" + x._2._1 + "\t" +  x._2._2 +  "\n"))
        writer.close()
    }
}

