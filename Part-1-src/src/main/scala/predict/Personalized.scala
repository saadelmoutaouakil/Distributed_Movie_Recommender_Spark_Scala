package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._


class PersonalizedConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Personalized extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new PersonalizedConf(args) 
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()
  
  // Compute here
  /*
  println("Testing Personalized ...")
  println("value is " + compute_MAE_by_predictor(train,test,similarity_prediction_method))
  println("similarity user 1 - user 2 " + similarity_user1_user2(train))
  println("Prediction user 1 - item 1 " + personalized_prediction_user_1_item_1(train))

*/



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
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "P.1" -> ujson.Obj(
          "1.PredUser1Item1" -> uniform_prediction_user_1_item_1(train), // Prediction of item 1 for user 1 (similarity 1 between users)
          "2.OnesMAE" -> compute_MAE_by_predictor(train,test,uniform_prediction_method)     // MAE when using similarities of 1 between all users
        ),
        "P.2" -> ujson.Obj(
          "1.AdjustedCosineUser1User2" -> similarity_user1_user2(train), // Similarity between user 1 and user 2 (adjusted Cosine)
          "2.PredUser1Item1" -> personalized_prediction_user_1_item_1(train),  // Prediction item 1 for user 1 (adjusted cosine)
          "3.AdjustedCosineMAE" -> compute_MAE_by_predictor(train,test,similarity_prediction_method) // MAE when using adjusted cosine similarity
        ),
        "P.3" -> ujson.Obj(
          "1.JaccardUser1User2" -> jacc_user1_user2(train), // Similarity between user 1 and user 2 (jaccard similarity)
          "2.PredUser1Item1" -> predict_jacc_user1_item1(train),  // Prediction item 1 for user 1 (jaccard)
          "3.JaccardPersonalizedMAE" -> compute_MAE_by_predictor(train,test,Jaccard_prediction_method) // MAE when using jaccard similarity
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
