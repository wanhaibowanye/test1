import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2019/4/26 0026.
  */
object TestMain {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\InstallPackage\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "D:\\test.txt"
    val rdd = sc.textFile(path).flatMap(line => {line.split(",")}).map((_, 1))

    rdd.groupByKey().map(x => {
      val key = x._1
      val value = x._2.sum
      (key, value)
    }).collect()
      .sortBy(_._2)
      .foreach(println(_))

    rdd.reduceByKey(_ + _).collect().sortBy(_._2).foreach(println(_))// 测试
  }

}
