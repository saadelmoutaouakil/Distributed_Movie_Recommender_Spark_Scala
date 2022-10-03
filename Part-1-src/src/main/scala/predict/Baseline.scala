package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  // For these questions, data is collected in a scala Array 
  // to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()


val measurement_global_average_method = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  compute_MAE_by_predictor(train,test,global_average_method)
}))
val timings_global_average_method = measurement_global_average_method.map(t => t._2) // Retrieve the timing measurementsù
val output_global_average_method = measurement_global_average_method.map(x => x._1)


val measurement_user_average_method = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  compute_MAE_by_predictor(train,test,user_average_method)
}))
val timings_user_average_method = measurement_user_average_method.map(t => t._2) // Retrieve the timing measurementsù
val output_user_average_method = measurement_user_average_method.map(x => x._1)


val measurement_item_average_method = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  compute_MAE_by_predictor(train,test,item_average_method)
}))
val timings_item_average_method = measurement_item_average_method.map(t => t._2) // Retrieve the timing measurementsù
val output_item_average_method = measurement_item_average_method.map(x => x._1)


val measurement_baseline_method = (1 to conf.num_measurements()).map(x => timingInMs(() => {
  compute_MAE_by_predictor(train,test,baseline_method)
}))
val timings_baseline_method = measurement_baseline_method.map(t => t._2) // Retrieve the timing measurementsù
val output_baseline_method = measurement_baseline_method.map(x => x._1)
  


  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> global_average_rating_q1(train), // Datatype of answer: Double
          "2.User1Avg" -> user_1_average_rating(train),  // Datatype of answer: Double
          "3.Item1Avg" -> item_1_average_rating(train),   // Datatype of answer: Double
          "4.Item1AvgDev" -> item_1_deviation(train), // Datatype of answer: Double
          "5.PredUser1Item1" -> baseline_user1_item1(train) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> mean(output_global_average_method), // Datatype of answer: Double
          "2.UserAvgMAE" -> mean(output_user_average_method),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> mean(output_item_average_method),   // Datatype of answer: Double
          "4.BaselineMAE" -> mean(output_baseline_method)   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> mean(timings_global_average_method), // Datatype of answer: Double
            "stddev (ms)" -> std(timings_global_average_method) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> mean(timings_user_average_method), // Datatype of answer: Double
            "stddev (ms)" -> std(timings_user_average_method) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> mean(timings_item_average_method), // Datatype of answer: Double
            "stddev (ms)" -> std(timings_item_average_method) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> mean(timings_baseline_method), // Datatype of answer: Double
            "stddev (ms)" -> std(timings_baseline_method) // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}
