package test.distributed

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

class ExactTests extends AnyFunSuite with BeforeAndAfterAll {
  
   val separator = "\t"
   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : CSCMatrix[Double] = null
   var test2 : CSCMatrix[Double] = null
   var sc : SparkContext = null

   override def beforeAll {
     train2 = load(train2Path, separator, 943, 1682)
     test2 = load(test2Path, separator, 943, 1682)

     val spark = SparkSession.builder().master("local[2]").getOrCreate();
     spark.sparkContext.setLogLevel("ERROR")
     sc = spark.sparkContext
   }




   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // the corresponding application.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   def parallel_test_knn(br_user_average:org.apache.spark.broadcast.Broadcast[DenseVector[Double]],
   br_normalized_dev:org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]],
   br_knn:org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]],
   br_global_average:org.apache.spark.broadcast.Broadcast[Double],
   train2:CSCMatrix[Double]):Array[((Int,Int),Double)] = {
     return sc.parallelize(test2.activeKeysIterator.toIndexedSeq).map(x=>distributed_knn_prediction(x._1,x._2,br_user_average.value,br_normalized_dev.value,br_knn.value,br_global_average.value,train2)).collect()
   }
   test("kNN predictor with k=10") { 
      val conf_users = 943
      val conf_movies = 1682
      val conf_k = 10
      val global_average_val = global_average(train2)
      val br_global_average = sc.broadcast(global_average_val)
      val user_average = average_rating_per_user(train2)
      val br_user_average = sc.broadcast(user_average)
      val normalized_dev = normalized_deviations(user_average,train2)
      val br_normalized_dev = sc.broadcast(normalized_dev)
      val u_i_rui = u_i_rui_mapping(normalized_dev)
      val br_u_i_rui = sc.broadcast(u_i_rui)
      val topks = sc.parallelize(0 to conf_users - 1 ).map(x=>topk(x,br_u_i_rui,conf_k)).collect()
      val knn = build_knn(conf_users,topks)
      val br_knn = sc.broadcast(knn)
      val predictions = parallel_test_knn(br_user_average, br_normalized_dev,br_knn,br_global_average,train2)
      val mae_val = mae(test2,toCSCMatrix(predictions.toSeq,conf_users,conf_movies))


     // Similarity between user 1 and itself
     assert(within(knn(0,0), 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     assert(within(knn(0,863), 0.2423, 0.0001))

     // Similarity between user 1 and 886
     assert(within(knn(0,885), 0.0, 0.0001))

     // Prediction user 1 and item 1
     assert(within(distributed_knn_prediction(0,0,br_user_average.value,br_normalized_dev.value,br_knn.value,br_global_average.value,train2)._2, 4.3190, 0.0001))

     // Prediction user 327 and item 2
     assert(within(distributed_knn_prediction(326,1,br_user_average.value,br_normalized_dev.value,br_knn.value,br_global_average.value,train2)._2, 2.6994, 0.0001))

     // MAE on test
     assert(within(mae_val, 0.8287, 0.0001)) 
   } 
}
