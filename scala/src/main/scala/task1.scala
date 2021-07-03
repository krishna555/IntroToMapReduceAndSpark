import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.jackson.Serialization
import java.io.PrintWriter
import java.io.File
object task1 {

    def taskBFilterAPI(review_data_record: JValue, year: String): Boolean = {
        implicit val formats = DefaultFormats
        (review_data_record \ "date").extract[String].substring(0, 4) == year
    }

    def removePunctuations(text: String, punctuations: Set[Char]): String= {
        text.map((ch) => {
            if (punctuations.contains(ch)) ' '
            else if (ch.isLetter || ch == ' ') ch
            else ' '
        })
    }
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Task1").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val review_data_path = args(0)
        val output_path = args(1)
        val stopwords_path = args(2)
        val year = args(3)
        val m = args(4).toInt
        val n = args(5).toInt
        // println(Paths.get(".").toAbsolutePath)
        val stopwords_set = Source.fromFile(stopwords_path).getLines.map((line) => line.trim()).toSet
        val review_data_rdd: org.apache.spark.rdd.RDD[JValue] = sc.textFile(review_data_path).map((row) => parse(row)).cache()

        // Do task A:
        val a_result = review_data_rdd.count()

        // Do Task B:
        // val b_result = review_data_rdd.filter((review) => (review \ "date").extract[String].substring(0, 4) == year).count()

        val b_result = review_data_rdd.filter((review) => taskBFilterAPI(review, year)).count()
        // Do Task C:
        val c_result = review_data_rdd.map((review) => {
            implicit val formats = DefaultFormats
            (review \ "user_id").extract[String]
        }).distinct().count()

        val d_result = review_data_rdd
            .map((review) => {
                implicit val formats = DefaultFormats
                ((review \  "user_id").extract[String], 1)
            })
            .reduceByKey((val1, val2) => val1 + val2)
            .takeOrdered(m)(Ordering[(Int, String)].on(x => (-x._2, x._1)))
            .map((item) => item.productIterator.toList)

        // Do Task E:
        val punctuations = Set('(', '[', ',', '.', '!', '?', ':', ';', ']', ')')
        val e_result_tuples = review_data_rdd
            .map((json_record) => {
                implicit val formats = DefaultFormats
                removePunctuations((json_record \ "text").extract[String], punctuations)
            })
            .flatMap((text) => text.split(" +"))
            .map((word) => (word.toLowerCase(), 1))
            .reduceByKey((val1, val2) => val1 + val2)
            .filter((tup) => {
                tup._1 != null && tup._1 != "" && !stopwords_set.contains(tup._1)
            })
            .takeOrdered(n)(Ordering[Int].on((x => -x._2)))
        val e_result = e_result_tuples.map(tup => tup._1)

        val result = scala.collection.immutable.Map(
            "A" -> a_result,
            "B" -> b_result,
            "C" -> c_result,
            "D" -> d_result,
            "E" -> e_result
        )

        implicit val formats = org.json4s.DefaultFormats
        val writer = new PrintWriter(new File(output_path))
        writer.write(Serialization.write(result))
        writer.close()
    }
}