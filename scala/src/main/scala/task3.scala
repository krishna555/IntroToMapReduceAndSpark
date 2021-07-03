import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}

class CustomPartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts

    def custom_hashing_logic(value: Any) = {
        val str_val = value.asInstanceOf[String]
        str_val.charAt(0).toInt
    }
    override def getPartition(key: Any): Int = {
        return custom_hashing_logic(key) % numParts
    }
}

object task3 {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Task2").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val reviewFilePath = args(0)
        val outputFilePath = args(1)
        val partitionType = args(2)
        val num_partitions = args(3).toInt
        val n = args(4).toInt

        val reviewDataRdd = sc.textFile(reviewFilePath).map((row) => parse(row))
        val bidCntRdd = reviewDataRdd.map((json_record) => {
            implicit val formats = DefaultFormats
            ((json_record \ "business_id").extract[String], 1)
        })
        def constructOutputDataFromRdd(rdd: RDD[(String, Int)]) = {
            val numPartitions = rdd.getNumPartitions
            val numItems = rdd.glom().map(_.length).collect()
            val resData = rdd
                .reduceByKey((val1, val2) => val1 + val2)
                .filter((tup) => tup._2 > n)
                .collect()
                .map((item) => item.productIterator.toList)
            val result = scala.collection.immutable.Map(
                "n_partitions" -> numPartitions,
                "n_items" -> numItems,
                "result" -> resData
            )
            result
        }

        def writeOutput(output_file_path: String, result: Map[String, Any]) = {
            implicit val formats = org.json4s.DefaultFormats
            val writer = new PrintWriter(new File(output_file_path))
            writer.write(Serialization.write(result))
            writer.close()
        }

        if (partitionType != "default") {
            val customPartsRdd = bidCntRdd.partitionBy(new CustomPartitioner(num_partitions))
            val outputData = constructOutputDataFromRdd(customPartsRdd)
            writeOutput(outputFilePath, outputData)
        }
        else {
            val outputData = constructOutputDataFromRdd(bidCntRdd)
            writeOutput(outputFilePath, outputData)
        }
    }
}