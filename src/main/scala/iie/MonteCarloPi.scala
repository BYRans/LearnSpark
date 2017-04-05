package iie
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by BYRans on 2017/3/24.
  * Email: dingyu.sdu@gmail.com
  * Blog: http://www.cnblogs.com/BYRans
  * GitHub: https://github.com/BYRans
  */

object MonteCarloPi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("App")
    val sc = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toString.toInt else 2
    val square = Math.min(100000000 * slices,Int.MaxValue)
    val circle = sc.parallelize(1 until square.toInt,slices).map(num => {
      val x = Math.random() * 2 - 1
      val y = Math.random() * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }
    ).reduce((a,b) => a+b)
    val pi: Double = 4.0*circle/square
    println("pi is : " + pi)
  }
}