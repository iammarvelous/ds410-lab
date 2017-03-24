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

        // Parse Lines and Extract Tokens
        val lines = sc.textFile("hdfs:/ds410/tweets/nyc-twitter-data-2013.csv")
		val texts = lines.map(line => line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")).map(cs => (if (cs.size < 7) ""; else cs(6)))
		val words = texts.flatMap(_.split("[ .,?!:\"]"))

        // Filter out Hash Tags
        val hashTags = words.filter(x => x.length() > 0 && x(0) == '#')

        // Get Hash Tag Frequency
        val hashKeyValue = hashTags.map(x => (x, 1))
        val hashFreq = hashKeyValue.reduceByKey((x,y) => x+y)

        // Sort Hash Tag Frequency, Get Top 100
        val top100 = hashFreq.collect.toSeq.sortWith(_._2 > _._2).take(100)

        // Write Output File
        val writer = new PrintWriter(new File("output.txt"))
        top100.foreach(x => writer.write(x._1 + "\t" + x._2 + "\n"))
        writer.close()
    }
}

