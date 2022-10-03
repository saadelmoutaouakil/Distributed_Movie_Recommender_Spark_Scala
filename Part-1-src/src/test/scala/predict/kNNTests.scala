package test.predict

import org.scalatest._
import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._
import ujson._

class kNNTests extends AnyFunSuite with BeforeAndAfterAll {

   val separator = "\t"
   var spark : org.apache.spark.sql.SparkSession = _

   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : Array[shared.predictions.Rating] = null
   var test2 : Array[shared.predictions.Rating] = null

   var adjustedCosine : Map[Int, Map[Int, Double]] = null

   override def beforeAll {
       Logger.getLogger("org").setLevel(Level.OFF)
       Logger.getLogger("akka").setLevel(Level.OFF)
       spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
       spark.sparkContext.setLogLevel("ERROR")

       // For these questions, train and test are collected in a scala Array
       // to not depend on Spark
       train2 = load(spark, train2Path, separator).collect()
       test2 = load(spark, test2Path, separator).collect()
   }

   // All the functions definitions for the tests below (and the tests in other suites) 
   // should be in a single library, 'src/main/scala/shared/predictions.scala'.

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // src/main/scala/predict/Baseline.scala.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("kNN predictor with k=10") { 
     // Create predictor on train2

     // Similarity between user 1 and itself
     assert(k_similarity_between_users(train2,1,1,10) == 0.0)
 
     // Similarity between user 1 and 864
     assert(inRange(k_similarity_between_users(train2,1,864,10),-1.0, 1.0))

     // Similarity between user 1 and 886
     assert(inRange(k_similarity_between_users(train2,1,886,10),-1.0, 1.0))

     // Prediction user 1 and item 1
     assert(inRange(prediction_user_item(train2,1,1,10),1.0, 5.0))

     kNN_mae(train2,test2,k_similarity_prediction_method,k)


     // MAE on test2 
     val mae = kNN_mae(train,test,k_similarity_prediction_method,10)
     assert(within(mae, 0.8287, 0.0001))
   } 

   test("kNN Mae") {
     // Compute MAE for k around the baseline MAE
     
     // Ensure the MAEs are indeed lower/higher than baseline
     assert(mae < compute_MAE_by_predictor(train2,test2,baseline_method))
   }
}
