package iie

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.{HiveContext, InsertIntoHiveTable}
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
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val urm = sc.textFile("hdfs://m02:8020/user/rans/user_ratedmovies.dat").cache()
    val mas = sc.textFile("hdfs://m02:8020/user/rans/movie_actors.dat").cache()
    val mds = sc.textFile("hdfs://m02:8020/user/rans/movie_directors.dat").cache()

    processU2M(urm, hc)
    processM2A(mas, hc)
    processM2D(mds, hc)

  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  def variance[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  def processU2M(urm: RDD[String], hc: HiveContext) {

    // 将原始数据的前3列取出，过滤掉脏数据，并将第3列转为Double
    val u2mTmp = urm.map(_.split("\t")).filter(_.length >= 3).map(x => (x(0), x(1), x(2))).filter(x => scala.util.Try(x._3.toDouble).isSuccess).map(x => (x._1, x._2, x._3.toDouble))

    // 求每个user的打分平均值
    val meanRDD = u2mTmp.map(x => (x._1, (x._3, 1))).reduceByKey {
      case ((x1, y1), (x2, y2))
      => (x1 + x2, y1 + y2)
    }.map { case (k, (sum, count)) => (k, sum / count) }

    // 求每个user的打分的标准差
    val variRDD = u2mTmp.map(x => (x._1, x._3)).join(meanRDD).map {
      case (k, (value, mean))
      => (k, ((value - mean) * (value - mean), 1))
    }.reduceByKey {
      case ((vm, i), (vm2, i2))
      => (vm + vm2, i + i2)
    }.map {
      case (k, (vm, count))
      => (k, math.sqrt(vm / count))
    }

    // 定义UtoM类，这个类在RDD转为DataFrame时需要用，Spark需要将该类作为模板，将RDD装为DataFrame
    case class UtoM(user: String, movie: String, weight: Double)
    import hc.implicits._
    // 计算Sigmoid之后，转为DataFrame
    val u2m = u2mTmp.map(x => (x._1, (x._2, x._3))).join(variRDD).map {
      case (user, ((movie, stanDev), rate)) => (user, (movie, rate, stanDev))
    }.join(meanRDD).map {
      case (user, ((movie, rate, stanDev), mean)) => (user, movie, (rate - mean) / stanDev)
    }.map(x => UtoM(x._1, x._2, 1.0 / (1 + math.exp(-x._3)))).toDF("user", "movie", "weight")
    // 将DataFrame注册为临时表，然后调用hc的sql接口将u2m持久化到Hive表中。
    u2m.registerTempTable("u2mDF")
    hc.sql("insert overwrite table rans.u2m select * from u2mDF")

  }

  def processM2A(mas: RDD[String], hc: HiveContext) {
    case class MtoA(movie: String, actor: String, weight: Double)
    import hc.implicits._
    var m2a = mas.map(_.split("\t")).filter(_.length >= 4).filter(x => scala.util.Try(x(3).toDouble).isSuccess).map(x => MtoA(x(0), x(1), x(3).toDouble)).toDF("movie", "actor", "weight")
    m2a.registerTempTable("m2aDF")
    hc.sql("insert overwrite table rans.m2a select * from m2aDF")
  }

  def processM2D(mds: RDD[String], hc: HiveContext): Unit = {
    case class MtoD(movie: String, director: String, weight: Double)
    import hc.implicits._
    var m2d = mds.filter(x => !x.contains("movieID")).map(_.split("\t")).filter(_.length >= 3).map(x => MtoD(x(0), x(1), 1.0)).toDF("movie", "director", "weight")
    m2d.registerTempTable("m2dDF")
    hc.sql("insert overwrite table rans.m2d select * from m2dDF")
  }

}

