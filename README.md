Authors :  
Saad El Moutaouakil  
Nicolas Jimenez  
  
  
  
In this project, we built a distributed recommender system for Movies.  
In Part 1,  we implemented a simple baseline prediction for recommendation then distributed it with Spark.  
We then compared the quality of its predictions to a second personalized approach based on similarities and k-NN.  
We finally measured the CPU time to develop insights in the system costs of the prediction methods.  
    
       
In part 2, we parallelized the computation of similarities by leveraging the Breeze linear algebra library for Scala, effectively using
more efficient low-level routines.    
We also measured how well our Spark implementation scales when adding more executors.   
Moreover, We implemented a version of approximate k-NN that could be useful in very large datasets.    
We finally computed economic ratios to choose the most appropriate infrastructure for our needs.  
