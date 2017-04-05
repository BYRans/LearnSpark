package iie

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object Test {
  /** Usage: HdfsTest [file] */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JL")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val sqlc = new SQLContext(sc)
    val u2m = hc.sql("select * from rans.u2m")
    val m2a = hc.sql("select * from rans.m2a")

    val m2aRdd = sqlc.sql("select * from rans.m2a")
    val u2mRdd = sqlc.sql("select * from rans.u2m")

    // u_m.toJavaRDD

    val Wus = calc("u1", "m2", u2m, m2a, hc)


  }

  def calc(s: String, t: String, u2m: DataFrame, m2a: DataFrame, hc: HiveContext) = {
    val s = "u1"
    val t = "m2"
    // 目标为计算U(s)-M-A-M'(t),先计算U-M
    // 计算1/|O(U1|R1),核心为计算出度
    val WsRel_um_out_degree = 1.0 / u2m.filter(u2m("user").equalTo("u1")).count()
    // 计算从s点出发涉及到的所有movie
    val movies = u2m.filter(u2m("user").equalTo("u1")).select("movie").distinct()
    // 计算公式最后是过滤涉及到的M-A边，设(s,t)间路线为U-M-A-M'，这里求的就是M-A和A-M',这里的A-M'路径对应WsRel表达式求解最后一步的A-M'(t)是否通联
    // 注意这里的join条件本应该写为‘m2a.join(movies, m2a("movie") === movies("movie"), "Left")’，但是这么写会产生4列，所以才使用了Seq的这个方式
    val conPathMA = m2a.join(movies, Seq("movie"), "Left")
    // conPathMA为所有可能用到的MA边，现在从公式的最后一步开始计算，先要计算conPath这些边中A与t相连的边（t为某一个movie，A即为该movie的所有actor）,
    // 所以接下来计算的就是t的所有actor与conPathMA中actor的交集
    val tActors = hc.sql("select distinct actor from rans.m2a where movie = " + "'" + t + "'")
    val conActors = tActors.join(conPathMA, Seq("actor"), "Left")

    val WsRel_w_wsrel = u2m.filter(u2m("user").equalTo(s)).select(u2m("weight"))

    val rdd = u2m.toJavaRDD

  }

  /*
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val u2m = hc.sql("select * from rans.u2m")
    val m2a = hc.sql("select * from rans.m2a")
    val movies = u2m.filter(u2m("user").equalTo("u1")).select("movie").distinct()

    val connectedPathMA = m2a.join(movies, m2a("movie") === movies("movie"), "Left")
  */
}







