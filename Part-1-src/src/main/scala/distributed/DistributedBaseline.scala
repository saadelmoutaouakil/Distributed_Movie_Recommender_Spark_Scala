package distributed

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val master = opt[String](default=Some(""))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object DistributedBaseline extends App {
  var conf = new Conf(args) 

  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = if (conf.master() != "") {
    SparkSession.builder().master(conf.master()).getOrCreate()
  } else {
    SparkSession.builder().getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator())
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator())





  
  val measurement_baseline_method = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    compute_MAE_by_predictor_distributed(train,test,baseline_method_distributed)
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
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Master" -> conf.master(),
          "4.Measurements" -> conf.num_measurements()
        ),
        "D.1" -> ujson.Obj(
          "1.GlobalAvg" -> global_average_rating_distributed(train), // Datatype of answer: Double
          "2.User1Avg" -> user_1_average_rating_distributed(train),  // Datatype of answer: Double
          "3.Item1Avg" -> item_1_average_rating_distributed(train),   // Datatype of answer: Double
          "4.Item1AvgDev" -> item_1_deviation_distributed(train), // Datatype of answer: Double,
          "5.PredUser1Item1" -> baseline_user1_item1_distributed(train), // Datatype of answer: Double
          "6.Mae" -> mean(output_baseline_method) // Datatype of answer: Double
        ),
        "D.2" -> ujson.Obj(
          "1.DistributedBaseline" -> ujson.Obj(
            "average (ms)" -> mean(timings_baseline_method), // Datatype of answer: Double
            "stddev (ms)" -> std(timings_baseline_method) // Datatype of answer: Double
          )            
        )
      )
      val json = write(answers, 4)

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
