package iie

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object WsRel {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JL")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val u2m = hc.sql("select * from rans.u2m")
    val m2a = hc.sql("select * from rans.m2a")
    val m2d = hc.sql("select * from rans.m2d")

    val s = "61674"
    val t = "364"

    // U-M-A-M的WsRel
    val Wumam = calcWsRel_m2a(s, t, u2m, m2a, hc)

    // U-M-D-M的WsRel
    val Wumdm = calcWsRel_m2d(s, t, u2m, m2d, hc)

    val Wpi = calcPathWeight(u2m, m2a, m2d, hc)

    // 计算两个路径加权后的值（现在有bug）
    val result = Wumam * Wpi._1 + Wumdm * Wpi._2

  }

  def calcWsRel_m2a(s: String, t: String, u2m: DataFrame, m2a: DataFrame, hc: HiveContext) = {
    // 从《算法思想》文档中WsRel（U1,M2）|R1R2R3) 推导公式的最后一步往上进行计算
    // 先计算[ 1/3【w(M1,a1)*WsRel(a1,M2)+w(M1,a2)*WsRel(a2,M2)+ w(M1,a3)*WsRel(a3,M2)]】+。。。，这里的【】中的和其实就是：
    //    1.	是路径U-M-A-M' 上   M-A的权重的和再乘以M的出度分之一
    //    2.	计算时先把M-A-M'路径过滤出来   不要没有A-M'的路径
    // ---------------------------
    // 接下来开始计算
    // 计算从s点出发涉及到的所有movie
    val movies = u2m.filter(u2m("user").equalTo(s)).select("movie").distinct()
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
    val sum_w = M_A.groupBy(M_A("movie")).sum("weight").withColumnRenamed("sum(weight)", "wsum")
    // 将sum_w和m2a_out_degree_1cent合并，方便后续的操作
    val tmp_sumw_1cent = sum_w.join(m2a_out_degree_1cent, Seq("movie"), "Left")
    // 下面求出的是出度分之一 乘以 权重和，这里的权重和即路径U-M-A-M' 上 M-A的权重的和，这里的M-A必须存在A-Mt边
    val WsResl_w_MAMt = tmp_sumw_1cent.select(tmp_sumw_1cent.col("movie"), tmp_sumw_1cent.col("wsum").*(tmp_sumw_1cent.col("degree"))).withColumnRenamed("(wsum * degree)", "wma_am")
    // ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑ End
    // ---------------------------

    // 这里是对U-M这一步进行计算
    // 计算1/|O(U1|R1),核心为计算出度
    val WsRel_um_out_degree = 1.0 / u2m.filter(u2m("user").equalTo(s)).count()
    // 下面是计算U-M-A-M'的U-M边
    // 先计算M-A-M'种所有的M集合，然后基于M集合和U，来计算有效的U-M集合（有效是指，U-M为s-t的一部分）
    val conMovie = M_A.select("movie").distinct()
    val conUM = conMovie.join(u2m.filter(u2m("user").equalTo(s)), Seq("movie"), "Left").select("user", "movie", "weight")

    val wu1_s = WsResl_w_MAMt.join(conUM, Seq("movie"), "Left")
    val result_WsResl = wu1_s.select(wu1_s.col("wma_am").*(wu1_s.col("weight"))).withColumnRenamed("(wma_am * weight)", "wmam_w")
    val resultTmp = result_WsResl.select(sum("wmam_w").*(WsRel_um_out_degree)).first().get(0)
    var stWsRel_m2a = 0.0
    if (resultTmp != null)
      stWsRel_m2a = resultTmp.toString.toDouble
    stWsRel_m2a
  }

  // 这里之所以要把m2a与m2d的计算WsRel的方法分开，是因为：方法里有些地方指定了表名、指定了表内的字段名，而m2a与m2d两个表的表名与字段名有区别
  def calcWsRel_m2d(s: String, t: String, u2m: DataFrame, m2d: DataFrame, hc: HiveContext) = {
    val movies = u2m.filter(u2m("user").equalTo(s)).select("movie").distinct()
    val conPathMD = movies.join(m2d, Seq("movie"), "Left")
    val tDirectors = hc.sql("select distinct director from rans.m2d where movie = " + "'" + t + "'")
    val M_A_Mt = tDirectors.join(conPathMD, Seq("director"), "Left")
    val A_Mt = M_A_Mt.filter(M_A_Mt("movie").equalTo(t))
    val M_A = M_A_Mt.filter(M_A_Mt("movie").notEqual(t))
    val m2d_out_degree_1cent = hc.sql("select movie,1.0/count(*) as degree from rans.m2d group by movie")
    val sum_w = M_A.groupBy(M_A("movie")).sum("weight").withColumnRenamed("sum(weight)", "wsum")
    val tmp_sumw_1cent = sum_w.join(m2d_out_degree_1cent, Seq("movie"), "Left")
    val WsResl_w_MAMt = tmp_sumw_1cent.select(tmp_sumw_1cent.col("movie"), tmp_sumw_1cent.col("wsum").*(tmp_sumw_1cent.col("degree"))).withColumnRenamed("(wsum * degree)", "wma_am")
    val WsRel_um_out_degree = 1.0 / u2m.filter(u2m("user").equalTo(s)).count()
    val conMovie = M_A.select("movie").distinct()
    val conUM = conMovie.join(u2m.filter(u2m("user").equalTo(s)), Seq("movie"), "Left").select("user", "movie", "weight")
    val wu1_s = WsResl_w_MAMt.join(conUM, Seq("movie"), "Left")
    val result_WsResl = wu1_s.select(wu1_s.col("wma_am").*(wu1_s.col("weight"))).withColumnRenamed("(wma_am * weight)", "wmam_w")
    val resultTmp = result_WsResl.select(sum("wmam_w").*(WsRel_um_out_degree)).first().get(0)
    var stWsRel_m2d = 0.0
    if (resultTmp != null)
      stWsRel_m2d = resultTmp.toString.toDouble
    stWsRel_m2d
  }

  def calcPathWeight(u2m: DataFrame, m2a: DataFrame, m2d: DataFrame, hc: HiveContext) = {
    // P1=U-(r1)-M-(r2)-A-(r3)-M  P2=U-(r1)-M-(r4)-A-(r5)-M

    // >>>>>>>>> 求S(R1) Start
    // 求|O(U|R1)|，可以理解为所有u2m边数除以总的user数
    val our1 = u2m.count.toDouble./(u2m.select("user").distinct().count().toDouble)
    // 求|I(M|R1)|，可以理解为所有u2m边数除以总的movie数
    val imr1 = u2m.count.toDouble./(u2m.select("movie").distinct().count().toDouble)
    val Sr1 = 1.0 / math.sqrt(our1 * imr1)
    // 求S(R1) end <<<<<<<<<<<<<<<<<<<<<<<

    // >>>>>>>>> 求S(R2) Start
    // 求|O(U|R2)|，可以理解为所有m2a边数除以总的movie数
    val our2 = m2a.count.toDouble./(m2a.select("movie").distinct().count().toDouble)
    // 求|I(M|R2)|，可以理解为所有u2m边数除以总的movie数
    val imr2 = m2a.count.toDouble./(m2a.select("actor").distinct().count().toDouble)
    val Sr2 = 1.0 / math.sqrt(our2 * imr2)
    // 求S(R2) end <<<<<<<<<<<<<<<<<<<<<<<

    // >>>>>>>>> 求S(R3) Start
    // 求|O(U|R3)|，可以理解为所有m2a边数除以总的actor数
    val our3 = m2a.count.toDouble./(m2a.select("actor").distinct().count().toDouble)
    // 求|I(M|R3)|，可以理解为所有m2a边数除以总的movie数
    val imr3 = m2a.count.toDouble./(m2a.select("movie").distinct().count().toDouble)
    val Sr3 = 1.0 / math.sqrt(our3 * imr3)
    // 求S(R3) end <<<<<<<<<<<<<<<<<<<<<<<

    // >>>>>>>>> 求S(R4) Start
    val our4 = m2d.count.toDouble./(m2d.select("movie").distinct().count().toDouble)
    val imr4 = m2d.count.toDouble./(m2d.select("director").distinct().count().toDouble)
    val Sr4 = 1.0 / math.sqrt(our4 * imr4)
    // 求S(R4) end <<<<<<<<<<<<<<<<<<<<<<<

    // >>>>>>>>> 求S(R5) Start
    val our5 = m2d.count.toDouble./(m2d.select("director").distinct().count().toDouble)
    val imr5 = m2d.count.toDouble./(m2d.select("movie").distinct().count().toDouble)
    val Sr5 = 1.0 / math.sqrt(our5 * imr5)
    // 求S(R5) end <<<<<<<<<<<<<<<<<<<<<<<

    // 求S(P1)、S(P2)
    val Sp1 = Sr1 * Sr2 * Sr3
    val Sp2 = Sr1 * Sr4 * Sr5

    // 求I(P1)、I(P1)
    val Ip1 = Sp1 / 4.0
    val Ip2 = Sp2 / 4.0

    // 求两个路径的权重
    val Wp1 = Ip1 / (Ip1 + Ip2)
    val Wp2 = Ip2 / (Ip1 + Ip2)

    // scala中最后一行的结果就是返回值
    (Wp1, Wp2)
  }


  /*
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    val u2m = hc.sql("select * from rans.u2m")
    val m2a = hc.sql("select * from rans.m2a")
    val s = "61674"
    val t = "364"
    val movies = u2m.filter(u2m("user").equalTo(s)).select("movie").distinct()
    val conPathMA = movies.join(m2a, Seq("movie"), "Left")
    val tActors = hc.sql("select distinct actor from rans.m2a where movie = " + "'" + t + "'")
    val M_A_Mt = tActors.join(conPathMA, Seq("actor"), "Left")
    val A_Mt = M_A_Mt.filter(M_A_Mt("movie").equalTo(t))
    val M_A = M_A_Mt.filter(M_A_Mt("movie").notEqual(t))
    val m2a_out_degree_1cent = hc.sql("select movie,1.0/count(*) as degree from rans.m2a group by movie")
    val sum_w = M_A.groupBy(M_A("movie")).sum("weight").withColumnRenamed("sum(weight)","wsum")
    val tmp_sumw_1cent = sum_w.join(m2a_out_degree_1cent, Seq("movie"), "Left")
    val WsResl_w_MAMt = tmp_sumw_1cent.select(tmp_sumw_1cent.col("movie"),tmp_sumw_1cent.col("wsum").*(tmp_sumw_1cent.col("degree"))).withColumnRenamed("(wsum * degree)","wma_am")
    val WsRel_um_out_degree = 1.0 / u2m.filter(u2m("user").equalTo(s)).count()
    val conMovie = M_A.select("movie").distinct()
    val conUM = conMovie.join(u2m.filter(u2m("user").equalTo(s)),Seq("movie"),"Left").select("user","movie","weight")
    val wu1_s = WsResl_w_MAMt.join(conUM,Seq("movie"),"Left")
    val result_WsResl = wu1_s.select(wu1_s.col("wma_am").*(wu1_s.col("weight"))).withColumnRenamed("(wma_am * weight)","wmam_w")
    val result = result_WsResl.select(sum("wmam_w").*(WsRel_um_out_degree)).show

  */
}







