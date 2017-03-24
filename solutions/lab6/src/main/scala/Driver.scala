import java.io.File
import scala.util.Try
import java.util.Arrays
import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import scala.collection.JavaConversions._
import scala.collection.JavaConversions

object Demo {
    // Application Specific Variables
	private final val SPARK_MASTER = "yarn-client"
	private final val APPLICATION_NAME = "demo"

	// HDFS Configuration Files
	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

	// Spark Context & Configurations
	final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
	final val sc = new SparkContext(conf)

    def main(args: Array[String]): Unit = {
        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Load & Parse Iris Dataset
        val lines = sc.textFile("/ds410/lab5/iris.data")
        val samples  = lines.map(line => line.split(",").slice(0,4).map(_.toDouble)).zipWithIndex().map(sample => (sample._2, sample._1))

		// Define Cluster Counts
		val clusters = List(3, 6, 9)

		// Define Centers
		var for_centers = List[Array[(Int, List[Double])]]()
		var dis_centers = List[Array[(Int, List[Double])]]()

		// Perform Iterative K-Means
		for (i <- clusters) {
			val k_for = new Kmeans(i, 4)
			k_for.for_run(samples, 100)
			for_centers :+= k_for.centers

			val k_dis = new Kmeans(i, 4)
			k_dis.dist_run(samples, 100)
			dis_centers :+= k_dis.centers
		}

		// Output For-Loop Centers
		var writer = new PrintWriter(new File("for_centers.txt"))
		for (i <- for_centers) {
			writer.write("[CLUSTER]\n")
			i.foreach(x => x._2.foreach(y => writer.write(x._1 + "\t" + y + "\n")))
			writer.write("\n")
		}
        writer.close()

		// Output Dist-Loop Centers
		writer = new PrintWriter(new File("dist_centers.txt"))
		for (i <- dis_centers) {
			writer.write("[CLUSTER]\n")
			i.foreach(x => x._2.foreach(y => writer.write(x._1 + "\t" + y + "\n")))
			writer.write("\n")
		}
		writer.close()
    }
}

