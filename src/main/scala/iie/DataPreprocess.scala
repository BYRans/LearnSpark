package iie

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BYRans on 2017/3/22.
  * Email: dingyu.sdu@gmail.com
  * Blog: http://www.cnblogs.com/BYRans
  * GitHub: https://github.com/BYRans
  */

object DataPreprocess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("App")
    val sc = new SparkContext(conf)
    val urm = sc.textFile("hdfs://m02:8020/user/rans/user_ratedmovies.dat").cache()
    val mas = sc.textFile("hdfs://m02:8020/user/rans/movie_actors.dat").cache()
    processU2M(urm)
    processM2A(mas)
  }

  def processU2M(urm: RDD[String]) {
    val u2m = urm.map(_.split("\t")).filter(_.length >= 3).map(x => x(0) + "\t" + x(1) + "\t" + x(2))
    // 如果已经存在结果目录，则删除
    val outputPath = new Path("hdfs://m02:8020/user/rans/u2m")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://m02:8020"), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(outputPath)) hdfs.delete(outputPath, true)
    // 将结果存储到HDFS指定目录上，注意Spark只能指定存储目录，目录里有两类文件，一个为：_SUCCESS，其余为part-0000*，方法repartition(1)就是将结果写到1个文件里
    u2m.repartition(1).saveAsTextFile("hdfs://m02:8020/user/rans/u2m")
  }

  def processM2A(mas: RDD[String]) {
    var m2a = mas.map(_.split("\t")).filter(_.length >= 4).map(x => x(0) + "\t" + x(1) + "\t" + x(3))
    // 如果已经存在结果目录，则删除
    val outputPath = new Path("hdfs://m02:8020/user/rans/m2a")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://m02:8020"), new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(outputPath)) hdfs.delete(outputPath, true)
    // 将结果存储到HDFS指定目录上，注意Spark只能指定存储目录，目录里有两类文件，一个为：_SUCCESS，其余为part-0000*，方法repartition(1)就是将结果写到1个文件里
    m2a.repartition(1).saveAsTextFile("hdfs://m02:8020/user/rans/m2a")
  }


}