package sparkTest


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

import org.apache.spark.SparkContext, org.apache.spark.SparkConf,   org.apache.spark.sql.hive.HiveContext


object sparksession {
      def main(args: Array[String]): Unit = {
  
        
        
        /* Using sparksession */

  val warehouseLocation = "hdfs://quickstart.cloudera:8020/user/hive/warehouse/";
  val spark = SparkSession.builder().master("local") .config("spark.sql.warehouse.dir", warehouseLocation).config("hive.metastore.uris", "thrift://quickstart.cloudera:9083")
  .enableHiveSupport().getOrCreate();
  
  import spark.implicits._
  import spark.sql


 val sqlDF = sql("select customer_id from customers order by customer_id limit 8");
 var  arr1List : java.util.List[Double] = new java.util.ArrayList[Double]();
 val numofInput = 3;
  
 sqlDF.collect().foreach { row => 
  arr1List.add(row.getInt(0));
  }
 
  
  println(arr1List);
 var FeatureArray  = new  Array[Double](numofInput);
 var LabelArray  = new  Array[Double](numofInput);

            
            
   for( i <- 0 to arr1List.size()-1){
     if ( i > arr1List.size()- (numofInput +1) ){
          return;
        }
      for( j <- 0  to numofInput -1) {
        arr1List.get(i)
        FeatureArray(j) = arr1List.get(i+j);
        LabelArray(j) = arr1List.get(i+j+1);
      }
      
        println("Feature: " );
       FeatureArray.foreach { row => println(row)}
      println("Label: " );

       LabelArray.foreach { row => println(row)}
       println("========" );


   }
 
 
  
 
 
 }
      
  
   //Using hiveContext 
        
   val conf = new SparkConf().setMaster("local").setAppName("scala spark")
    val sc = new SparkContext(conf)
   val hiveContext = new HiveContext(sc);
  hiveContext.setConf("hive.metastore.uris", "thrift://quickstart.cloudera:9083");
  val sqlDF =   hiveContext.sql("select * from orders");
  sqlDF.show()
   
   
      
      
}
