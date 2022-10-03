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

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val k = opt[Int]()
  val json = opt[String]()
  val users = opt[Int]()
  val movies = opt[Int]()
  val separator = opt[String](default=Some("\t"))
  val replication = opt[Int](default=Some(1))
  val partitions = opt[Int](default=Some(1))
  val master = opt[String]()
  val num_measurements = opt[Int](default=Some(1))
  verify()
}

object Approximate {
  def main(args: Array[String]) {
    var conf = new Conf(args)

    // Remove these lines if encountering/debugging Spark
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = conf.master.toOption match {
      case None => SparkSession.builder().getOrCreate();
      case Some(master) => SparkSession.builder().master(master).getOrCreate();
    }
    val sc = spark.sparkContext

    println("")
    println("******************************************************")

    // conf object is not serializable, extract values that
    // will be serialized with the parallelize implementations
    val conf_users = conf.users()
    val conf_movies = conf.movies()
    val conf_k = conf.k()

    println("Loading training data")
    val train = loadSpark(sc, conf.train(), conf.separator(), conf.users(), conf.movies())
    val test = loadSpark(sc, conf.test(), conf.separator(), conf.users(), conf.movies())
    var knn : CSCMatrix[Double] = null

    println("Partitioning users")
    var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      conf.users(), 
      conf.partitions(), 
      conf.replication()
    )


      var global_average_val : Double = 0.0
      var br_global_average : org.apache.spark.broadcast.Broadcast[Double] = null
      var user_average : DenseVector[Double]= null
      var br_user_average : org.apache.spark.broadcast.Broadcast[DenseVector[Double]] =null
      var normalized_dev : CSCMatrix[Double] =null
      var br_normalized_dev : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]] = null
      var u_i_rui : CSCMatrix[Double]  = null
      var br_u_i_rui : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]] = null  
      var topks : Array[Map[Int,Seq[(Int,Double)]]] = null
    var res : Map[Int,Seq[(Int,Double)]] = null
    var final_knn : CSCMatrix[Double]= null
    var predicted : Array [((Int,Int),Double)]= null
    var mae_val : Double = 0.0

    
    val measurements = (1 to scala.math.max(1,conf.num_measurements()))
      .map(_ => timingInMs( () => {
       global_average_val = global_average(train)
       br_global_average = sc.broadcast(global_average_val)
       user_average = average_rating_per_user(train)
       br_user_average = sc.broadcast(user_average)
       normalized_dev = normalized_deviations(user_average,train)
       br_normalized_dev = sc.broadcast(normalized_dev)
       u_i_rui = u_i_rui_mapping(normalized_dev)
       br_u_i_rui = sc.broadcast(u_i_rui)    
       topks = sc.parallelize(partitionedUsers).map(s => getSimilaritiesPartition(s, partitionMatrixByUsers(u_i_rui,s), conf_k)).collect
       res = topks.flatten.groupBy(_._1).mapValues(x => x.map(y => y._2).flatten.distinct.sortWith(_._2 > _._2).take(conf_k).toSeq)
       final_knn = build_knn_app(conf_users, res)
       predicted = sc.parallelize(test.activeKeysIterator.toIndexedSeq).map(x=>distributed_knn_prediction(x._1,x._2,br_user_average.value,br_normalized_dev.value,final_knn,br_global_average.value, train)).collect()
      mae_val = mae(test,toCSCMatrix(predicted.toSeq,conf_users,conf_movies))
      mae_val
    }))
    //val mae = measurements(0)._1
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
            "num_measurements" -> ujson.Num(conf.num_measurements()),
            "partitions" -> ujson.Num(conf.partitions()),
            "replication" -> ujson.Num(conf.replication()) 
          ),
          "AK.1" -> ujson.Obj(
            "knn_u1v1" -> ujson.Num(final_knn(0, 0)),
            "knn_u1v864" -> ujson.Num(final_knn(0,863)),
            "knn_u1v344" -> ujson.Num(final_knn(0,343)),
            "knn_u1v16" -> ujson.Num(final_knn(0,15)),
            "knn_u1v334" -> ujson.Num(final_knn(0, 333)),
            "knn_u1v2" -> ujson.Num(final_knn(0, 1))
          ),
          "AK.2" -> ujson.Obj(
            "mae" -> ujson.Num(mae_val) 
          ),
          "AK.3" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)),
            "stddev (ms)" -> ujson.Num(std(timings))
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
