
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import scala.io.Source
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization

import java.io.{File, PrintWriter}
import scala.collection.mutable
object task2 {
    def readFile(inputFilePath: String, requiredKeys: Array[String]) = {
        Source.fromFile(inputFilePath).getLines
            .map((line) => line.trim())
            .map((line) => parse(line))
            .map((json_record) => {
                implicit val formats = DefaultFormats
                ((json_record \ requiredKeys(0)).extract[String], (json_record \ requiredKeys(1)).extract[String])
            }).toList
    }

    def process_no_spark(review_file_path: String, business_file_path: String, n: Int) = {
        val review_records = readFile(review_file_path, Array("business_id", "stars"))
        val business_records = readFile(business_file_path, Array("business_id", "categories"))
        val bid_to_stars_map = review_records.groupBy(_._1).mapValues(value => value.map(_._2))
        val business_id_to_categories_map =
            business_records
                .filter((tuple) => tuple._2 != null && tuple._2 != "")
                .groupBy(_._1)
                .mapValues(value => {
                    val categories = value.map(_._2)
                    categories.flatMap((category) => category.split(",").map(x => x.trim()))
                })
        val x = business_id_to_categories_map.toSeq.flatMap { case (key, list) => list.map(key -> _)}
        val categories_to_business_id_map = x.groupBy(_._2).mapValues((value) => value.map(_._1)).toMap
                var ans_map: mutable.LinkedHashMap[String, Double] = mutable.LinkedHashMap()
                categories_to_business_id_map.foreach((kvTuple) => {
                  val category = kvTuple._1
                  val sum_and_counts = kvTuple._2.map((business_id) => {
                      var sum_of_stars = 0.0
                      var count_of_stars = 0.0
                      if (bid_to_stars_map.contains(business_id)) {
                          val stars = bid_to_stars_map(business_id).map((value) => {
                              value.toFloat
                          })
                          sum_of_stars = stars.sum
                          count_of_stars = stars.size
                      }
                      (sum_of_stars, count_of_stars)
                  }).toList
                    val sum = sum_and_counts.map(_._1).sum
                    val count = sum_and_counts.map(_._2).sum
                    val average =  if (count > 0) sum/count else 0
                    ans_map += (category -> average)
                })
                val category_to_avg = ans_map.toList
                val resTuplesOrdered = category_to_avg
                    .sortBy(t => (-t._2, t._1))
                    .map((t) => (t._1, BigDecimal(t._2).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble)).take(n)
                resTuplesOrdered.map((item) => item.productIterator.toList)
    }

    def process_with_spark(review_file_path: String, business_file_path: String, n: Int, sc: SparkContext) = {
        val review_data_rdd = sc.textFile(review_file_path).map((row) => parse(row))
        val business_data_rdd =sc.textFile(business_file_path).map((row) => parse(row))

        val review_bid_stars_rdd = review_data_rdd.map((json_record) => {
            implicit val formats = DefaultFormats
            ((json_record \ "business_id").extract[String], (json_record \ "stars").extract[String].toFloat)
        })
        val business_bid_categories_rdd =business_data_rdd.map((json_record) => {
            implicit val formats = DefaultFormats
            ((json_record \ "business_id").extract[String], (json_record \ "categories").extract[String])
        })
        val stars_and_count_rdd = review_bid_stars_rdd.groupByKey().map((bid_and_stars) => {
            (bid_and_stars._1, ((bid_and_stars._2).sum, (bid_and_stars._2).size))
        })

        val categories_rdd = business_bid_categories_rdd.filter((tuple)=> tuple._2 != null && tuple._2 != "" )

        stars_and_count_rdd.join(categories_rdd)
            .map((tup) => {
                val categories = tup._2._2
                val sanitized_categories = categories.split(",").map((category) => category.trim())
                (sanitized_categories, tup._2._1)
            })
            .flatMap((tup) => {
                tup._1.map((category) => {
                    (category, tup._2)
                })
            })
            .reduceByKey((tup1, tup2) => {
                (tup1._1 + tup2._1, tup1._2 + tup2._2)
            })
            .mapValues((sum_and_count_tuple) => {
                (sum_and_count_tuple._1 / sum_and_count_tuple._2)
            })
            .sortBy(t => (-t._2, t._1))
            .take(n)
            .map((t) => (t._1, BigDecimal(t._2).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble)).take(n)
            .map((item) => item.productIterator.toList)
    }

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Task2").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val review_file_path: String = args(0)
        val business_file_path: String = args(1)
        val output_file_path: String = args(2)
        val spark_mode: String = args(3)
        val n: Int = args(4).toInt

        if (spark_mode == "spark") {
            val output = process_with_spark(review_file_path, business_file_path, n, sc)
            val result = scala.collection.immutable.Map(
                "result" -> output
            )
            implicit val formats = org.json4s.DefaultFormats
            val writer = new PrintWriter(new File(output_file_path))
            writer.write(Serialization.write(result))
            writer.close()
        }
        else {
            val output_no_spark = process_no_spark(review_file_path, business_file_path, n)
            val result = scala.collection.immutable.Map(
                "result" -> output_no_spark
            )
            implicit val formats = org.json4s.DefaultFormats
            val writer = new PrintWriter(new File(output_file_path))
            writer.write(Serialization.write(result))
            writer.close()
        }
    }
}
