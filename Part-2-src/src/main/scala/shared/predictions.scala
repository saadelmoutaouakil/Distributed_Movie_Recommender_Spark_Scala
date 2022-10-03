package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

package object predictions
{
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else { 
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
    }
  }


  def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
                 case None => false })
      .map({ case Some(x) => x
             case None => ((-1, -1), -1) }).collect()

    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
    for ((k,v) <- ratings) {
      v match {
        case d: Double => {
          val u = k._1
          val i = k._2
          builder.add(u, i, d)
        }
      }
    }
    return builder.result
  }

  def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
       .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
    (0 to (nbUsers-1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }

  //-------------------------------------------Optimizing------------------------------

      def scale(x: Double, average_rating: Double) = {
      if(x>average_rating) 5.0-average_rating
      else if (x<average_rating) average_rating-1.0
      else 1.0
  }

  def average_rating_per_user(train : CSCMatrix[Double]) : DenseVector[Double]= {
    val ones_vector = DenseVector.ones[Double](train.cols)
    val nb_ratings = train.mapValues(x => if(x ==0.0) 0.0 else 1.0) * ones_vector
    val sum_ratings = train * ones_vector
    return sum_ratings /:/ nb_ratings 
  }

  def normalized_deviations(avg_rating_per_user : DenseVector[Double], train : CSCMatrix[Double]) : CSCMatrix[Double]= {
    val builder = new CSCMatrix.Builder[Double](rows = train.rows, cols = train.cols)
    for((k,v)<-train.activeIterator){
      val u = k._1
      val i = k._2
      val normalized_dev =  (train(u,i)-avg_rating_per_user(u))/(scale(train(u,i),avg_rating_per_user(u)).toDouble)
      builder.add(u,i,normalized_dev)
    }
    return builder.result()
  }

  def u_i_rui_mapping(normalized_devs : CSCMatrix[Double]) : CSCMatrix[Double]= {
    val builder = new CSCMatrix.Builder[Double](rows = normalized_devs.rows, cols = normalized_devs.cols)
    val ones_vector = DenseVector.ones[Double](normalized_devs.cols)
    val sq_denom = sqrt(pow(normalized_devs, 2) * ones_vector)
    for((k,v)<-normalized_devs.activeIterator){
      val u = k._1
      val i = k._2
      if(sq_denom(u) == 0) {builder.add(u,i,0.0)}
      else builder.add(u,i,v/sq_denom(u))
    }
    return builder.result()
  }

  def u_v_suv_mapping(u_i_rui_mapping:CSCMatrix[Double]) : CSCMatrix[Double]  = {
    val user_to_user_similarity = u_i_rui_mapping * u_i_rui_mapping.t
    for(u<-0 until u_i_rui_mapping.rows){
      user_to_user_similarity(u,u) = 0.0
    }
    return user_to_user_similarity
  }

  def similarity_between_users_k(u_v_suv:CSCMatrix[Double],k:Int,user1 : Int, user2 : Int) : Double = {
    val similarity_first_user = u_v_suv(user1-1,0 to u_v_suv.cols-1).t
    for(i <- argtopk(similarity_first_user,k)){
      if(i == user2-1) {
        return similarity_first_user(i)
      }
    }
    return 0.0
  }

  def knn_sparse_matrix(u_v_suv:CSCMatrix[Double],k:Int): CSCMatrix[Double] = {
     val builder = new CSCMatrix.Builder[Double](rows = u_v_suv.rows, cols = u_v_suv.cols)
     for(i <- 0 until u_v_suv.rows){
       val similarity_user_i=  u_v_suv(i,0 to u_v_suv.cols - 1).t
       for(j <- argtopk(similarity_user_i,k)){
         builder.add(i,j,similarity_user_i(j))
       }
     }
     return builder.result()
  }

  def k_compute_weighted_sum_dev(user : Int, item : Int, normalized_dev : CSCMatrix[Double],knn_matrix: CSCMatrix[Double],train :CSCMatrix[Double] ) : Double =  {

    val neighbors = knn_matrix(user,0 to knn_matrix.cols - 1).t.toDenseVector
    val item_devs = normalized_dev(0 to normalized_dev.rows-1,item).toDenseVector
    val abs_neighbors = abs(neighbors)
    val users_rating_item = train(0 to train.rows-1,item).toDenseVector.mapValues(x => if(x==0.0) 0.0 else 1.0)
    val num = (neighbors dot item_devs)
    val denom = (abs_neighbors dot users_rating_item)
    if(denom == 0.0) return 0.0
    else return num/denom

  }

  def knn_method_prediction(test:CSCMatrix[Double],
   user_average : DenseVector[Double], normalized_dev : CSCMatrix[Double],
   knn_matrix : CSCMatrix[Double],train : CSCMatrix[Double]): CSCMatrix[Double] = {
     val builder = new CSCMatrix.Builder[Double](rows = test.rows, cols = test.cols)
     val ones_vector = DenseVector.ones[Double](train.rows)

     var acc = 0.0
     var nb_non_zero_val = 0.0

     for((k,v)<-train.activeIterator){
      val u = k._1
      val i = k._2

      acc = acc + train(u,i)
      if(train(u,i) != 0) {
        nb_non_zero_val = nb_non_zero_val + 1 
      }

     }
     val global_average = acc / nb_non_zero_val
     
     for((k,v)<-test.activeIterator){
      val u = k._1
      val i = k._2
      val u_avg = if (user_average(u) != 0.0) user_average(u) else global_average
      val weighted_sum = k_compute_weighted_sum_dev(u,i,normalized_dev,knn_matrix,train)
      if(weighted_sum != 0.0) {
        val prediction = u_avg + weighted_sum * scale(u_avg+weighted_sum,u_avg)
        builder.add(u,i,prediction)
      }
      else builder.add(u,i,u_avg)
    }
    
    return builder.result()


  }

  def mae(test: CSCMatrix[Double], predicted_values : CSCMatrix[Double]) : Double = {
    return sum(abs(predicted_values-test))/test.activeSize.toDouble
  }

  //---------------------------------------DISTRIBUTED FUNCTIONS----------------------------------------

      def similarity_one_user_all_others(user:Int,u_i_rui_mapping : CSCMatrix[Double]) : DenseVector[Double] = {
      val user_to_user_similarity = u_i_rui_mapping * u_i_rui_mapping(user,0 to u_i_rui_mapping.cols - 1 ).t
      // Don't forget to zero user-to-same-user similarity !!!
      user_to_user_similarity(user) = 0.0
      return user_to_user_similarity.toDenseVector
    }



    def build_knn(nb_users : Int,topks: Seq[(Int,Seq[(Int,Double)])]) : CSCMatrix[Double] = {
      val builder = new CSCMatrix.Builder[Double](rows = nb_users, cols = nb_users)
      for (element <- topks){
        for (pair <- element._2) builder.add(element._1,pair._1,pair._2)
      }
      return builder.result()

    }

    def global_average(train:CSCMatrix[Double]) : Double = {
      var acc = 0.0
      var nb_non_zero_val = 0.0

      for((k,v)<-train.activeIterator){
        val u = k._1
        val i = k._2

        acc = acc + train(u,i)
        if(train(u,i) != 0) {
          nb_non_zero_val = nb_non_zero_val + 1 
        }

      }

      val global_average = acc / nb_non_zero_val
      global_average
    }


    def distributed_knn_prediction(u : Int, i : Int,
    user_average : DenseVector[Double], normalized_dev : CSCMatrix[Double],
    knn_matrix : CSCMatrix[Double],global_average: Double,train:CSCMatrix[Double]): ((Int,Int),Double) = {

      
      val u_avg = if (user_average(u) != 0.0) user_average(u) else global_average
      val weighted_sum = k_compute_weighted_sum_dev(u,i,normalized_dev,knn_matrix,train)
      if(weighted_sum != 0.0) {
        val prediction = u_avg + weighted_sum * scale(u_avg+weighted_sum,u_avg)
        return ((u,i),prediction)
      }
      else return ((u,i),u_avg)
    


    }


    def toCSCMatrix(tab: Seq[((Int,Int),Double)],nb_rows:Int,nb_cols:Int) : CSCMatrix[Double] = {
      val builder = new CSCMatrix.Builder[Double](rows = nb_rows, cols = nb_cols)
      for (element <- tab) builder.add(element._1._1,element._1._2,element._2)
      return builder.result()
    }

      def topk(u:Int,br_u_i_rui:org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]],k : Int) : (Int,Seq[(Int,Double)]) = {
      val broadcast_val = br_u_i_rui.value
      val similarity = similarity_one_user_all_others(u,broadcast_val)
      return (u,argtopk(similarity,k).map(v=>(v,similarity(v)))) 
    }

    def partitionMatrixByUsers(toPartitionMatrix : CSCMatrix[Double], subUsers: Set[Int]) : CSCMatrix[Double] = {
//Possible d'avoir plusieurs sequences dans un même worker ?
val builder = new CSCMatrix.Builder[Double](rows = subUsers.size, cols = toPartitionMatrix.cols)
var index = 0
for(i <- subUsers){
(0 to toPartitionMatrix.cols -1).map(j => builder.add(index, j, toPartitionMatrix(i, j)))
index += 1
}
return builder.result
}

def build_knn_app(nb_users : Int, topks: Map[Int,Seq[(Int,Double)]]) : CSCMatrix[Double] = {
val builder = new CSCMatrix.Builder[Double](rows = nb_users, cols = nb_users)
for (element <- topks){
for (pair <- element._2) builder.add(element._1,pair._1,pair._2)
}
return builder.result()

}

def topk_approximate(u:Int, list_users : List[Int], matrix_users : CSCMatrix[Double], k: Int) : (Int,Seq[(Int,Double)]) = {
val similarity = similarity_one_user_all_others(list_users.indexOf(u),matrix_users)
if(k <= similarity.size)
return (u,argtopk(similarity,k).map(v=>(list_users(v),similarity(v)))) 
else 
return (u,argtopk(similarity,similarity.size).map(v=>(list_users(v),similarity(v)))) 
//get the index of the matrix v so instead of v list_users(v)
}

def getSimilaritiesPartition(set_users : Set[Int], matrix_users : CSCMatrix[Double], k: Int) : Map[Int,Seq[(Int,Double)]] = {
val list_user = set_users.toList  
val topks = list_user.map(u => topk_approximate(u, list_user, matrix_users, k))
return topks.toMap
}




}


