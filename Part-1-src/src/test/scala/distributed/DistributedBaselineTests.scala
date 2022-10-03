package test.distributed

import org.scalatest._

import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._

class DistributedBaselineTests extends AnyFunSuite with BeforeAndAfterAll {

   val separator = "\t"
   var spark : org.apache.spark.sql.SparkSession = _

   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : org.apache.spark.rdd.RDD[shared.predictions.Rating] = null
   var test2 : org.apache.spark.rdd.RDD[shared.predictions.Rating] = null

   override def beforeAll {
       Logger.getLogger("org").setLevel(Level.OFF)
       Logger.getLogger("akka").setLevel(Level.OFF)
       spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
       spark.sparkContext.setLogLevel("ERROR")
       train2 = load(spark, train2Path, separator)
       test2 = load(spark, test2Path, separator)
   }

   // All the functions definitions for the tests below (and the tests in other suites) 
   // should be in a single library, 'src/main/scala/shared/predictions.scala'.

   // Provide tests to show how to call your code to do the following tasks (each in with their own test):
   // each method should be invoked with a single function call. 
   // Ensure you use the same function calls to produce the JSON outputs in
   // src/main/scala/predict/Baseline.scala.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("Compute global average")                           { assert(inRange(global_average_rating_distributed(train2), 1.0, 5.0)) }
   test("Compute user 1 average")                           { assert(inRange(user_1_average_rating_distributed(train2), 1.0, 5.0)) }
   test("Compute item 1 average")                           { assert(inRange(item_1_average_rating_distributed(train2), 1.0, 5.0))  }
   test("Compute item 1 average deviation")                 { assert(inRange(item_1_deviation_distributed(train2), 0.0, 1.0))  } // ??? 
   test("Compute baseline prediction for user 1 on item 1") { assert(inRange(baseline_user1_item1_distributed(train2), 1.0, 5.0))  }

   // Show how to compute the MAE on all four non-personalized methods:
   // 1. There should be four different functions, one for each method, to create a predictor
   // with the following signature: ````predictor: (train: Seq[shared.predictions.Rating]) => ((u: Int, i: Int) => Double)````;
   // 2. There should be a single reusable function to compute the MAE on the test set, given a predictor;
   // 3. There should be invocations of both to show they work on the following datasets.
   test("MAE on all four non-personalized methods on data/ml-100k/u2.base and data/ml-100k/u2.test") {
    assert(global_average_method_distributed(train2,test2).filter{x => (!inRange(x,1.0,5.0))}.count == 0)
    assert(user_average_method_distributed(train2,test2).filter{x=> (!inRange(x,1.0,5.0))}.count == 0)
    assert(item_average_method_distributed(train2,test2).filter{x=> (!inRange(x,1.0,5.0))}.count == 0)
    assert(baseline_method_distributed(train2,test2).filter{x=> (!inRange(x,1.0,5.0))}.count == 0)

    // MAE should be in this order : Global average > User average > Item Average > Baseline Method
    assert(compute_MAE_by_predictor_distributed(train2,test2,global_average_method_distributed) > compute_MAE_by_predictor_distributed(train2,test2,user_average_method_distributed))
    assert(compute_MAE_by_predictor_distributed(train2,test2,user_average_method_distributed) > compute_MAE_by_predictor_distributed(train2,test2,item_average_method_distributed))
    assert(compute_MAE_by_predictor_distributed(train2,test2,item_average_method_distributed) > compute_MAE_by_predictor_distributed(train2,test2,baseline_method_distributed))
   }
}
