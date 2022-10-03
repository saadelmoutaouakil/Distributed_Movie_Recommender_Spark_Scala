package shared

import predict._
import org.apache.spark.rdd.RDD
package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0
  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble)
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def load(spark : org.apache.spark.sql.SparkSession,  path : String, sep : String) : org.apache.spark.rdd.RDD[Rating] = {
       val file = spark.sparkContext.textFile(path)
       return file
         .map(l => {
           val cols = l.split(sep).map(_.trim)
           toInt(cols(0)) match {
             case Some(_) => Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
             case None => None
           }
       })
         .filter({ case Some(_) => true 
                   case None => false })
         .map({ case Some(x) => x 
                case None => Rating(-1, -1, -1)})
  }




def global_average_rating_q1(train: Seq[Rating])={
    train.map(x=>x.rating).foldLeft(0.0)(_+_)/train.size
  }

  def user_1_average_rating(train: Seq[Rating])={
    val user1_entries = train.filter(x=> x.user == 1).map(x=>x.rating)
    mean(user1_entries)
  }

  def item_1_average_rating(train: Seq[Rating])={
    val item1_entries = train.filter(x=> x.item == 1).map(x=>x.rating)
    mean(item1_entries)
  }

  def scale(x: Double, average_rating: Double) = {
    if(x>average_rating) 5.0-average_rating
    else if (x<average_rating) average_rating-1.0
    else 1.0
  }

  def getRating(train: Seq[Rating],u:Int,i:Int,averages:Map[Int,Double]) = {
    val filtered = train.filter(x => x.user == u && x.item == i).map(x=>x.rating)
    if (filtered.size > 0) {
    filtered(0)}
    else if (averages.contains(u)) averages.get(u).get
    else 100050.0 // Some big random number to spot errors
}

  def item_1_deviation(train: Seq[Rating])={
    val nb_users_rated_item_1 = train.filter(x=>x.item == 1).map(x=>x.user).toSet.size 
    val ratings_for_item_1 = train.filter(x=>x.item == 1).map(x=>(x.user,x.rating))
    val map_average_ratings_by_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    val normalized_deviation_for_item_1 = map_average_ratings_by_user.toList.map(x=>(getRating(train,x._1,1,map_average_ratings_by_user)-x._2)/scale(getRating(train,x._1,1,map_average_ratings_by_user),x._2))
    (normalized_deviation_for_item_1).sum/train.filter(x=>x.item == 1).size
  }

  def baseline_user1_item1(train: Array[Rating]) = {
    val user_1_avg = user_1_average_rating(train)
    val item_1_dev = item_1_deviation(train)
    user_1_avg + item_1_dev*scale((user_1_avg+item_1_dev),user_1_avg)
  }

  def global_average_method(train: Array[Rating],test: Array[Rating]) : Array[Double] ={
    val global_average = train.map(x=>x.rating).foldLeft(0.0)(_+_)/train.size
    test.map(x=>global_average)
  } 


  def user_average_method(train: Array[Rating],test: Array[Rating]) : Array[Double] ={
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    test.map(x=> avg_ratings_per_user.get(x.user).get)
  }



  def item_average_method(train: Array[Rating],test: Array[Rating]) : Array[Double] ={
    val global_average = global_average_method(train,test)(0)
    val items_averages = train.map(x=>(x.item,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    test.map(x=>items_averages.getOrElse(x.item,global_average))
  }




  def baseline_method(train: Array[Rating],test: Array[Rating]) : Array[Double] ={
    val global_average = global_average_method(train,test)(0)
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    val map_of_deviations = train.map(x=> {
      val tmp =  avg_ratings_per_user.get(x.user).get
      new Rating(x.user,
      x.item,
      (x.rating -tmp)/scale(x.rating,tmp).toDouble
      )
      })
    val map_of_normalized_deviations = map_of_deviations.groupBy(x=>x.item).mapValues(x=>mean(x.map(x=>x.rating)))

    val accs = test.map(x=>{
      val user_avg = avg_ratings_per_user.getOrElse(x.user,global_average)
      var t = 0.0
      val item_dev = map_of_normalized_deviations.get(x.item) match {
        case Some(value) =>  t = user_avg + value * scale((user_avg+value),user_avg) 
        case _  => t = avg_ratings_per_user.getOrElse(x.user,global_average)
      }
      t
    })
    accs
 }


 def compute_MAE_by_predictor(train: Array[Rating],test: Array[Rating],predictor: (Array[Rating],Array[Rating]) => Array[Double]): Double = {
   val predictions= predictor(train,test)
   mean(test.zip(predictions).map{case (x,y) => (x.rating-y).abs})
 }


 //----------------------------------------------------Distributed Baseline -----------------------------------------------------------------------

  def global_average_rating_distributed(train: RDD[Rating])={
    train.map(x=>x.rating).reduce(_+_)/train.count
  }

  def user_1_average_rating_distributed(train: RDD[Rating]) = {
    val user1_entries = train.filter(x=> x.user == 1).map(x=>x.rating)
    mean(user1_entries.collect)
  }

  def item_1_average_rating_distributed(train: RDD[Rating]) = {
    val item1_entries = train.filter(x=> x.item == 1).map(x=>x.rating)
    mean(item1_entries.collect)
  }

  def item_1_deviation_distributed(train: RDD[Rating]) = {
  val map_average_ratings_by_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2).toSeq)).collect().toMap
  val only_item_1 = train.filter(x=>x.item==1)
  only_item_1.persist()
  val normalized_deviation_for_item_1 = only_item_1.map(x=>{
    val user_avg = map_average_ratings_by_user.filter(y=>y._1 == x.user).get(x.user).get
    val tmp = (x.rating - user_avg) / scale(x.rating,user_avg)
    tmp
  })
  (normalized_deviation_for_item_1).reduce(_+_)/only_item_1.count
  }

  def baseline_user1_item1_distributed(train: RDD[Rating]) = {
    val user_1_avg = user_1_average_rating_distributed(train)
    val item_1_dev = item_1_deviation_distributed(train)
    user_1_avg + item_1_dev*scale((user_1_avg+item_1_dev),user_1_avg)
  }


    def global_average_method_distributed(train: RDD[Rating],test: RDD[Rating]): RDD[Double]={
      val global_average = train.map(x=>x.rating).reduce(_+_)/train.count
      test.map(x=>global_average)
    } 
   

  def user_average_method_distributed(train: RDD[Rating],test: RDD[Rating]): RDD[Double]={
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2).toSeq)).collect().toMap
    val values = test.map(
      x=> {
        val tmp = avg_ratings_per_user.get(x.user).get
        tmp}
      )
    values
  }


  def item_average_method_distributed(train: RDD[Rating],test: RDD[Rating]): RDD[Double]={
    val global_average = global_average_method_distributed(train,test).collect()(0)
    val items_averages = train.map(x=>(x.item,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2).toSeq)).collect().toMap
    val values = test.map(x=>(items_averages.getOrElse(x.item,global_average)))
    values
  }


  def baseline_method_distributed(train: RDD[Rating],test: RDD[Rating]): RDD[Double]={
    

    val global_average = global_average_method_distributed(train,test).collect()(0) 
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2).toSeq)).collect().toMap
    val map_of_deviations = train.map(x=> {
      val tmp =  avg_ratings_per_user.get(x.user).get
      new Rating(x.user,
      x.item,
      (x.rating -tmp)/scale(x.rating,tmp).toDouble
      )
      })
    val map_of_normalized_deviations = map_of_deviations.groupBy(x=>x.item).mapValues(x=>mean(x.map(x=>x.rating).toSeq)).collect().toMap
  
    val accs = test.map(x=>{
      val user_avg = avg_ratings_per_user.getOrElse(x.user,global_average)
      var t = 0.0
      val item_dev = map_of_normalized_deviations.get(x.item) match {
        case Some(value) =>  t = user_avg + value * scale((user_avg+value),user_avg) 
        case _  => t = avg_ratings_per_user.getOrElse(x.user,global_average)
      }
      t
    })
    accs
  }

  def compute_MAE_by_predictor_distributed(train: RDD[Rating],test: RDD[Rating],predictor: (RDD[Rating],RDD[Rating]) => RDD[Double]): Double = {
    val predictions= predictor(train,test)
    mean(test.zip(predictions).map{case (x,y) => (x.rating-y).abs}.collect())
  }


  //-----------------------------------------------------------------------------------------------------------------------------------------


 

 def sum_of_squares(items : Seq[Double]) = {
   val sum_of_squares = items.map(x=> x*x).sum
   math.sqrt(sum_of_squares)
 }




 def map_u_i_rui(train : Array[Rating])={
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  //Attention, cette fonction est utilisée 2 fois (Aussi dans similarity prediction method)
  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    })

    val denominator = map_of_deviations.groupBy{case ((x,y),z) => x}.mapValues(x=> sum_of_squares(x.map(_._2))) // Map (User) --> Denominateur
    map_of_deviations.toMap.map{case (key,value) => 
      if(denominator.getOrElse(key._1, 0.0) != 0.0) (key, (value / denominator.get(key._1).get.toDouble))
      else (key,0.0)
      }
  }


 def map_u_v_suv(train: Array[Rating],map_u_i_rui : Map[(Int,Int),Double])= {
  val train_grouped_by_user = train.groupBy(x=>x.user).mapValues(x => x.map(_.item))
  val possible_pairs = train_grouped_by_user.keySet.toList.combinations(2).map{ case Seq(x, y) => (x, y) }.toList // Liste des pairs
  val mm = possible_pairs.map{case (x,y) => ((x,y),train_grouped_by_user.get(x).get.toSet.intersect(train_grouped_by_user.get(y).get.toSet).toList)} // Map (u,v) -> Liste des items en commun
  //Pas besoin de getOrElse car tt les utilisateurs ont rate des items
  mm.map{ case (key,value) => 
    if(!value.isEmpty){
      val oo = value.map(x=> map_u_i_rui.get((key._1,x)).get*map_u_i_rui.get((key._2,x)).get)
      (key,oo.sum.toDouble)
    }
    else (key, 0.0)
    }
 }



def compute_weighted_sum_dev(user:Int, item: Int, train_grouped_by_items : Map[Int,Array[Int]], map_u_v_suv_val : Map[(Int,Int),Double], map_deviations : Map[(Int,Int),Double]) = {
  val interesting_users = train_grouped_by_items.get(item).get
  val ooegf = interesting_users.map{
    x => {
      val suv = map_u_v_suv_val.getOrElse((x,user), map_u_v_suv_val.get(user, x).get)
      val num =  suv * map_deviations.get((x,item)).get
      val denom = suv.abs

      if (suv != 0) {(num,denom)}
      else {
      (0.0,0.0)}}
  }
  val (numerator_list,denominator_list) = ooegf.unzip
  numerator_list.sum / denominator_list.sum.toDouble
}

import scala.collection.mutable.ListBuffer


def similarity_prediction_method(train:Array[Rating],test:Array[Rating]) = {
  val global_average = global_average_method(train,test)(0)
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  val map_u_i_rui_val = map_u_i_rui(train).toMap
  val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    }).toMap
  //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)
  val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user)) 
  //Attention, le map_of_normalized_deviation est ajouté mais il ajoute un passage sur la liste
  val accs = test.map(x=>{
    val user_avg = avg_ratings_per_user.getOrElse(x.user,global_average)
    //var t = 0.0
    if(train_grouped_by_items.keySet.contains(x.item)){
      val weighted_sum = compute_weighted_sum_dev(x.user, x.item, train_grouped_by_items,map_u_v_suv_val,map_of_deviations)
      if(weighted_sum != 0.0 ) {
        user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
        }
      else {    
      user_avg}
    }

    else {
      user_avg}
  })
  accs
}

def map_u_v_uniform(train: Array[Rating])= {
  val train_grouped_by_user = train.groupBy(x=>x.user).mapValues(x => x.map(_.item))
  val possible_pairs = train_grouped_by_user.keySet.toList.combinations(2).map{ case Seq(x, y) => (x, y) }.toList // Liste des pairs
  val mm = possible_pairs.map{case (x,y) => ((x,y),1.0)} // Map (u,v) -> Liste des items en commun
  mm.toMap
 }
def uniform_prediction_method(train:Array[Rating],test:Array[Rating])={
  val global_average = global_average_method(train,test)(0)
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  val map_u_v_uniform_val = map_u_v_uniform(train)
  
  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    }).toMap
  //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)
  val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user)) 
  //Attention, le map_of_normalized_deviation est ajouté mais il ajoute un passage sur la liste
  val accs = test.map(x=>{
    val user_avg = avg_ratings_per_user.getOrElse(x.user,global_average)
    //var t = 0.0
    if(train_grouped_by_items.keySet.contains(x.item)){
      val weighted_sum = compute_weighted_sum_dev(x.user, x.item, train_grouped_by_items,map_u_v_uniform_val,map_of_deviations)
      if(weighted_sum != 0.0 ) {
        user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
        }
      else {    
      user_avg}
    }

    else {
      user_avg}
  })
  accs
}


def uniform_prediction_user_1_item_1(train:Array[Rating]) = {
  val global_average = global_average_method(train,train)(0)
  val map_u_v_uniform_val = map_u_v_uniform(train)
 
  val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user))
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    }).toMap
  //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)

  val user_avg = avg_ratings_per_user.getOrElse(1,global_average)
  val weighted_sum = compute_weighted_sum_dev(1, 1, train_grouped_by_items,map_u_v_uniform_val,map_of_deviations)
  user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
}



def personalized_prediction_user_1_item_1(train:Array[Rating]) = {
  val global_average = global_average_method(train,train)(0)
  val map_u_i_rui_val = map_u_i_rui(train).toMap
  val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
  val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user))
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    }).toMap
  //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)

  val user_avg = avg_ratings_per_user.getOrElse(1,global_average)
  val weighted_sum = compute_weighted_sum_dev(1, 1, train_grouped_by_items,map_u_v_suv_val,map_of_deviations)
  user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
}

def similarity_user1_user2(train:Array[Rating]) = {
  val map_u_i_rui_val = map_u_i_rui(train).toMap
  val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
  map_u_v_suv_val.getOrElse((1,2),map_u_v_suv_val.get((2,1)).get)
}


def compute_jaccard_similarity(train : Array[Rating]) = {
  val train_grouped_by_user = train.groupBy(x=>x.user).mapValues(x => x.map(_.item))
  val possible_pairs = train_grouped_by_user.keySet.toList.combinations(2).map{ case Seq(x, y) => (x, y) }.toList // Liste des pairs
  val mm = possible_pairs.map{case (x,y) => ((x,y),(train_grouped_by_user.get(x).get.toSet.intersect(train_grouped_by_user.get(y).get.toSet).toList, 
  train_grouped_by_user.get(x).get.toSet.union(train_grouped_by_user.get(y).get.toSet).toList))}
  mm.map{ case (key,value) => 
    if(!value._1.isEmpty && !value._2.isEmpty){
      val oo = value._1.size.toDouble / value._2.size
      (key,oo)
    }
    else (key, 0.0)
    }.toMap
}


def Jaccard_prediction_method(train:Array[Rating],test:Array[Rating]) = {
  val global_average = global_average_method(train,test)(0)
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  val map_u_v_jacc = compute_jaccard_similarity(train)

  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    }).toMap
  //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)
  val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user)) 
  //Attention, le map_of_normalized_deviation est ajouté mais il ajoute un passage sur la liste
  val accs = test.map(x=>{
    val user_avg = avg_ratings_per_user.getOrElse(x.user,global_average)
    //var t = 0.0
    if(train_grouped_by_items.keySet.contains(x.item)){
      val weighted_sum = compute_weighted_sum_dev(x.user, x.item, train_grouped_by_items,map_u_v_jacc,map_of_deviations)
      if(weighted_sum != 0.0 ) {
        user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
        }
      else {    
      user_avg}
    }
    else {
      user_avg}
  })
  accs
}

def jacc_user1_user2(train:Array[Rating])={
  val map_u_v_jacc = compute_jaccard_similarity(train)
  map_u_v_jacc.getOrElse((1,2),map_u_v_jacc.get((2,1)).get)
}

def predict_jacc_user1_item1(train:Array[Rating]) = {
  val global_average = global_average_method(train,train)(0)
  val map_u_v_jacc = compute_jaccard_similarity(train)
  val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user))
  val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
  val map_of_deviations = train.map(x=> {
    val tmp =  avg_ratings_per_user.get(x.user).get
    ((x.user,
    x.item),
    (x.rating -tmp)/scale(x.rating,tmp).toDouble)
    }).toMap
  //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)

  val user_avg = avg_ratings_per_user.getOrElse(1,global_average)
  val weighted_sum = compute_weighted_sum_dev(1, 1, train_grouped_by_items,map_u_v_jacc,map_of_deviations)
  user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
}

//-----------------------------------------------------------------------------------------------------------------------------------------
import scala.collection.immutable.ListMap
def map_u_k_similarities(train: Array[Rating], map_u_v_suv_val: Map[(Int, Int), Double], k: Int) = {
  //traiter le cas ou les 2 ne sont pas les mêmes
  val complete_map_u_v_suv = map_u_v_suv_val.map{case ((u, v), similarity) =>((v, u), similarity)}
  
  (map_u_v_suv_val.++(complete_map_u_v_suv)).groupBy{case ((x,v), similarity) => x}.mapValues(x => 
    ListMap(x.toSeq.sortWith(_._2 > _._2):_*).take(k)) // Map u -> (((u,v) -> similarities) only for the k best of each user
  }

  def k_compute_weighted_sum_dev(user:Int, item: Int, train_grouped_by_items : Map[Int,Array[Int]], map_u_k_similarities_val : Map[(Int,Int),Double], map_deviations : Map[(Int,Int),Double]) = {
    val interesting_users = train_grouped_by_items.get(item).get
    val ooegf = interesting_users.map{
      x => {
        
        val suv = map_u_k_similarities_val.getOrElse((user,x),0.0)
        val num =  suv * map_deviations.get((x,item)).get
        val denom = suv.abs
  
        if (suv != 0) {(num,denom)}
        else {
        (0.0,0.0)}}
    }
    val (numerator_list,denominator_list) = ooegf.unzip
    val thesum = denominator_list.sum.toDouble
    if(thesum == 0.0) {0.0}
    else numerator_list.sum / thesum
  }

  import scala.collection.mutable.ListBuffer
  
  
  def k_similarity_prediction_method(train:Array[Rating],test:Array[Rating],k : Int) = {
   
    val global_average = global_average_method(train,test)(0)
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    val map_u_i_rui_val = map_u_i_rui(train).toMap
    val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
   



    val map_u_k_similarities_val = map_u_k_similarities(train, map_u_v_suv_val, k).values.flatten.toMap
   


   
    val map_of_deviations = train.map(x=> {
      val tmp =  avg_ratings_per_user.get(x.user).get
      ((x.user,
      x.item),
      (x.rating -tmp)/scale(x.rating,tmp).toDouble)
      }).toMap
    //Attention, cette fonction est utilisée 2 fois (Aussi dans map_u_i)
    val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user)) 
    //Attention, le map_of_normalized_deviation est ajouté mais il ajoute un passage sur la liste
    

    val accs = test.map(x=>{
      val user_avg = avg_ratings_per_user.getOrElse(x.user,global_average)
      //var t = 0.0
      val tim= System.nanoTime()
      if(train_grouped_by_items.keySet.contains(x.item)){
        val weighted_sum = k_compute_weighted_sum_dev(x.user, x.item, train_grouped_by_items,map_u_k_similarities_val,map_of_deviations)
        if(weighted_sum != 0.0 ) {

          user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)

          }
        else {  
        user_avg}






      }

      else {
        user_avg}
       
    })
    accs
  }

  def k_similarity_between_users(train:Array[Rating],user1: Int, user2: Int, k:Int) = {
    val map_u_i_rui_val = map_u_i_rui(train).toMap
    val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
    val map_u_k_similarities_val = map_u_k_similarities(train, map_u_v_suv_val, k).values.flatten.toMap
    map_u_k_similarities_val.getOrElse((user1, user2), 0.0)
  }
  

  def kNN_mae(train: Array[Rating],test: Array[Rating],predictor: (Array[Rating],Array[Rating],Int) => Array[Double], k:Int): Double = {
    val predictions= predictor(train,test,k)
    mean(test.zip(predictions).map{case (x,y) => (x.rating-y).abs})
  }

//------------------------------------------------------RECOMMANDER-------------------------------------------------------------------

def prediction_user_item(train: Array[Rating], user: Int, item: Int, k: Int) = {
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    val map_u_i_rui_val = map_u_i_rui(train).toMap
    val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
    val map_u_k_similarities_val = map_u_k_similarities(train, map_u_v_suv_val, k).values.flatten.toMap
    val map_of_deviations = train.map(x=> {
      val tmp =  avg_ratings_per_user.get(x.user).get
      ((x.user,
      x.item),
      (x.rating -tmp)/scale(x.rating,tmp).toDouble)
      }).toMap
    val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user)) 
    val user_avg = avg_ratings_per_user.get(user).get
    val weighted_sum = k_compute_weighted_sum_dev(user, item, train_grouped_by_items,map_u_k_similarities_val,map_of_deviations)
    user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
}

def recommander_to_user(train: Array[Rating], user: Int, k: Int) = {
    val avg_ratings_per_user = train.map(x=>(x.user,x.rating)).groupBy(_._1).mapValues(x=>mean(x.map(_._2)))
    val map_u_i_rui_val = map_u_i_rui(train).toMap
    val map_u_v_suv_val = map_u_v_suv(train,map_u_i_rui_val).toMap
    val map_u_k_similarities_val = map_u_k_similarities(train, map_u_v_suv_val, k).values.flatten.toMap
    val map_of_deviations = train.map(x=> {
      val tmp =  avg_ratings_per_user.get(x.user).get
      ((x.user,
      x.item),
      (x.rating -tmp)/scale(x.rating,tmp).toDouble)
      }).toMap
    val train_grouped_by_items = train.groupBy(x => x.item).mapValues(x => x.map(_.user)) 
    val user_avg = avg_ratings_per_user.get(user).get
    val already_seen_movies = train.filter(x=>x.user == user).map(_.item)
    val not_seen = train.filter(x => !already_seen_movies.contains(x.item))
    val all_Recommendations = not_seen.map(x=>{
        val weighted_sum = k_compute_weighted_sum_dev(user, x.item, train_grouped_by_items,map_u_k_similarities_val,map_of_deviations)
        if(weighted_sum != 0.0 ) {
          val tmp = user_avg + weighted_sum*scale(user_avg+weighted_sum,user_avg)
          (x.item,tmp)
          }
        else {  
        (x.item,user_avg)}
    })
  
    all_Recommendations.sortWith(_._2 > _._2).take(3)
}



}