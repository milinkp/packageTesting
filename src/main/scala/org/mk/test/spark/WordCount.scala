package org.mk.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) = {

    var conf = new SparkConf().setAppName("WordCount").setMaster("local")
    var sc=new SparkContext(conf);
    val test=sc.textFile("food.txt")
    test.flatMap { x => 
            x.split(" ")
      }
    .map { x => (x,1) }
    .reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")
    

  }
}