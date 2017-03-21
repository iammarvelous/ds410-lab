package lab7

import org.apache.spark.rdd.RDD

object Helper extends Serializable {
    def parseData(lines: RDD[String]): RDD[Map[String, Array[String]]] = {
        val rows = lines.map(line => line.split(','))
        val papers = rows.map(rowToPaper)
        return papers.filter(m => m.contains("A"))
    }

    // Input:  ["I:1", "N:a1", "N:a2", "A:blah"]
    // Output: {"I": [1], "N": ["a1", "a2"], "A": ["blah"]}
    def rowToPaper(row: Array[String]): Map[String, Array[String]]= {
        return row.groupBy(_(0).toString).map(x => (x._1, x._2.map(_.substring(2)).map(_.toLowerCase)))
    }
}
