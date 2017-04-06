package iie

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


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
    // 从《算法思想》文档中WsRel（U1,M2）|R1R2R3) 推导公式的最后一步往上进行计算
    // 先计算[ 1/3【w(M1,a1)*WsRel(a1,M2)+w(M1,a2)*WsRel(a2,M2)+ w(M1,a3)*WsRel(a3,M2)]】+。。。，这里的【】中的和其实就是：
    //    1.	是路径U-M-A-M' 上   M-A的权重的和再乘以M的出度分之一
    //    2.	计算时先把M-A-M'路径过滤出来   不要没有A-M'的路径
    // ---------------------------
    // 接下来开始计算
    // 计算从s点出发涉及到的所有movie
    val movies = u2m.filter(u2m("user").equalTo("u1")).select("movie").distinct()
    // 计算公式最后是过滤涉及到的M-A边，设(s,t)间路线为U-M-A-M'，这里求的就是M-A和A-M',这里的A-M'路径对应WsRel表达式求解最后一步的A-M'(t)是否通联,M-A包括A不与M'通联的情况
    // 注意这里的join条件本应该写为‘m2a.join(movies, m2a("movie") === movies("movie"), "Left")’，但是这么写会产生4列，所以才使用了Seq的这个方式
    val conPathMA = movies.join(m2a, Seq("movie"), "Left")
    // conPathMA为所有可能用到的MA边，现在从公式的最后一步开始计算，先要计算conPath这些边中A与t相连的边（t为某一个movie，A即为该movie的所有actor）,
    // 所以接下来计算的就是t的所有actor与conPathMA中actor的交集，得到的就是所有M-A、A-M'边
    val tActors = hc.sql("select distinct actor from rans.m2a where movie = " + "'" + t + "'")
    val M_A_Mt = tActors.join(conPathMA, Seq("actor"), "Left")
    // 下面是A-M'边
    val A_Mt = M_A_Mt.filter(M_A_Mt("movie").equalTo(t))
    // 下面是M-A边
    val M_A = M_A_Mt.filter(M_A_Mt("movie").notEqual(t))
    // ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑ 至此得到的A_Mt为U-M-A-M'路径上的A-M'
    // 下面计算的是每个movie的出度分之一，即U-M-A-M'路径上，所有M点的出度分之一，目的是为了计算出1/|O(Mi|R2)，即出度分之一
    val m2a_out_degree_1cent = hc.sql("select movie,1.0/count(*) as degree from rans.m2a group by movie")
    // 下面对U-M-A-M'路径上M-A-M'边集合中的M-A边按M分组，每个组内求权重w的和，再乘以对应M的出度分之一，从而求出出度分之一 与 A-Mt边存在的↓↓↓↓↓↓↓↓↓↓↓↓ Start
    val sum_w = M_A.groupBy(M_A("movie")).sum("weight").withColumnRenamed("sum(weight)","wsum")
    // 将sum_w和m2a_out_degree_1cent合并，方便后续的操作
    val tmp_sumw_1cent = sum_w.join(m2a_out_degree_1cent, Seq("movie"), "Left")
    // 下面求出的是出度分之一 乘以 权重和，这里的权重和即路径U-M-A-M' 上 M-A的权重的和，这里的M-A必须存在A-Mt边
    val WsResl_w_MAMt = tmp_sumw_1cent.select(tmp_sumw_1cent.col("movie"),tmp_sumw_1cent.col("wsum").*(tmp_sumw_1cent.col("degree"))).withColumnRenamed("(wsum * degree)","wma_am")
    // ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑ End
    // ---------------------------

    // 这里是对U-M这一步进行计算
    // 计算1/|O(U1|R1),核心为计算出度
    val WsRel_um_out_degree = 1.0 / u2m.filter(u2m("user").equalTo("u1")).count()
    // 下面是计算U-M-A-M'的U-M边
    // 先计算M-A-M'种所有的M集合，然后基于M集合和U，来计算有效的U-M集合（有效是指，U-M为s-t的一部分）
    val conMovie = M_A.select("movie").distinct()
    val conUM = conMovie.join(u2m.filter(u2m("user").equalTo("u1")),Seq("movie"),"Left").select("user","movie","weight")

    val wu1_s = WsResl_w_MAMt.join(conUM,Seq("movie"),"Left")
    val result_WsResl = wu1_s.select(wu1_s.col("wma_am").*(wu1_s.col("weight"))).withColumnRenamed("(wma_am * weight)","wmam_w")
    val result = result_WsResl.select(sum("wmam_w").*(WsRel_um_out_degree))
  }

  /*
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val u2m = hc.sql("select * from rans.u2m")
    val m2a = hc.sql("select * from rans.m2a")
    val s = "u1"
    val t = "m2"
    val movies = u2m.filter(u2m("user").equalTo("u1")).select("movie").distinct()
    val conPathMA = movies.join(m2a, Seq("movie"), "Left")
    val tActors = hc.sql("select distinct actor from rans.m2a where movie = " + "'" + t + "'")
    val M_A_Mt = tActors.join(conPathMA, Seq("actor"), "Left")
    val A_Mt = M_A_Mt.filter(M_A_Mt("movie").equalTo(t))
    val M_A = M_A_Mt.filter(M_A_Mt("movie").notEqual(t))
    val m2a_out_degree_1cent = hc.sql("select movie,1.0/count(*) as degree from rans.m2a group by movie")
    val sum_w = M_A.groupBy(M_A("movie")).sum("weight").withColumnRenamed("sum(weight)","wsum")
    val tmp_sumw_1cent = sum_w.join(m2a_out_degree_1cent, Seq("movie"), "Left")
    val WsResl_w_MAMt = tmp_sumw_1cent.select(tmp_sumw_1cent.col("movie"),tmp_sumw_1cent.col("wsum").*(tmp_sumw_1cent.col("degree"))).withColumnRenamed("(wsum * degree)","wma_am")
    val WsRel_um_out_degree = 1.0 / u2m.filter(u2m("user").equalTo("u1")).count()
    val conMovie = M_A.select("movie").distinct()
    val conUM = conMovie.join(u2m.filter(u2m("user").equalTo("u1")),Seq("movie"),"Left").select("user","movie","weight")
    val wu1_s = WsResl_w_MAMt.join(conUM,Seq("movie"),"Left")
    val result_WsResl = wu1_s.select(wu1_s.col("wma_am").*(wu1_s.col("weight"))).withColumnRenamed("(wma_am * weight)","wmam_w")
    val result = result_WsResl.select(sum("wmam_w").*(WsRel_um_out_degree))
  */
}







