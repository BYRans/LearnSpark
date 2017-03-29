package iie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BYRans on 2017/3/22.
  * Email: dingyu.sdu@gmail.com
  * Blog: http://www.cnblogs.com/BYRans
  * GitHub: https://github.com/BYRans
  */

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("App")
    val sc = new SparkContext(conf)
    val logData = sc.textFile("hdfs://kvdb07:8020/user/rans/test").cache()
    val wc = logData.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wc.collect().foreach(println(_))
  }
}