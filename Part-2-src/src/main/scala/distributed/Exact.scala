import org.rogach.scallop._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import ujson._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._

package distributed {

class ExactConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int](default=Some(10))
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String](default=Some("\t"))
  val master = opt[String]()
  val num_measurements = opt[Int](default=Some(1))
  verify()
}

object Exact {
  def main(args: Array[String]) {
    var conf = new ExactConf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = conf.master.toOption match {
      case None => SparkSession.builder().getOrCreate();
      case Some(master) => SparkSession.builder().master(master).getOrCreate();
    }
    spark.sparkContext.setLogLevel("ERROR")
    val sc = spark.sparkContext

    println("")
    println("******************************************************")
    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()

    println("Loading training data from: " + conf.train())
    val train = loadSpark(sc, conf.train(), conf.separator(), conf.users(), conf.movies())
    val test = loadSpark(sc, conf.test(), conf.separator(), conf.users(), conf.movies())

    //--------------------------------Values----------------------------
      var global_average_val : Double = 0.0
      var br_global_average : org.apache.spark.broadcast.Broadcast[Double] = null
      var user_average : DenseVector[Double]= null
      var br_user_average : org.apache.spark.broadcast.Broadcast[DenseVector[Double]] =null
      var normalized_dev : CSCMatrix[Double] =null
      var br_normalized_dev : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]] = null
      var u_i_rui : CSCMatrix[Double]  = null
      var br_u_i_rui : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]] = null
      var topks : Seq[(Int,Seq[(Int,Double)])] = null
      var knn : CSCMatrix[Double] = null
      var br_knn : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]] = null
      var predictions : Array [((Int,Int),Double)] = null
      var mae_val : Double = 0.0


    val measurements = (1 to scala.math.max(1,conf.num_measurements())).map(_ => timingInMs( () => {
       global_average_val = global_average(train)
       br_global_average = sc.broadcast(global_average_val)
       user_average = average_rating_per_user(train)
       br_user_average = sc.broadcast(user_average)
       normalized_dev = normalized_deviations(user_average,train)
       br_normalized_dev = sc.broadcast(normalized_dev)
       u_i_rui = u_i_rui_mapping(normalized_dev)
       br_u_i_rui = sc.broadcast(u_i_rui)
       topks = sc.parallelize(0 to conf_users - 1 ).map(x=>topk(x,br_u_i_rui,conf_k)).collect()
       knn = build_knn(conf_users,topks)
       br_knn = sc.broadcast(knn)
       predictions = sc.parallelize(test.activeKeysIterator.toIndexedSeq).map(x=>distributed_knn_prediction(x._1,x._2,br_user_average.value,br_normalized_dev.value,br_knn.value,br_global_average.value,train)).collect()
      mae_val = mae(test,toCSCMatrix(predictions.toSeq,conf_users,conf_movies))
      mae_val
    }))
    val timings = measurements.map(_._2)

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
            "train" -> ujson.Str(conf.train()),
            "test" -> ujson.Str(conf.test()),
            "k" -> ujson.Num(conf.k()),
            "users" -> ujson.Num(conf.users()),
            "movies" -> ujson.Num(conf.movies()),
            "master" -> ujson.Str(sc.getConf.get("spark.master")),
            "num-executors" -> ujson.Str(if (sc.getConf.contains("spark.executor.instances"))
                                            sc.getConf.get("spark.executor.instances")
                                         else
                                            ""),
            "num_measurements" -> ujson.Num(conf.num_measurements())
          ),
          "EK.1" -> ujson.Obj(
            "1.knn_u1v1" -> knn(0,0),
            "2.knn_u1v864" -> knn(0,863),
            "3.knn_u1v886" -> knn(0,885),
            "4.PredUser1Item1" -> distributed_knn_prediction(0,0,br_user_average.value,br_normalized_dev.value,br_knn.value,br_global_average.value,train)._2,
            "5.PredUser327Item2" -> distributed_knn_prediction(326,1,br_user_average.value,br_normalized_dev.value,br_knn.value,br_global_average.value,train)._2,
            "6.Mae" -> mae_val
          ),
          "EK.2" ->  ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          )
        )
        val json = write(answers, 4)
        println(json)
        println("Saving answers in: " + jsonFile)
        printToFile(json, jsonFile)
      }
    }

    println("")
    spark.stop()
  } 
}

}
