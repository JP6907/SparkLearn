package Serializable

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RuleSerTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName("RuleSerTest").setMaster("spark://116.56.140.131:50005")
                .setJars(List("E:\\SparkLearn\\target\\Sparklearn-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)

    val lines : RDD[String] = sc.textFile(args(0))

    val r = lines.map(word => {
      //情况1
      //val rules = new Rules
      val hostname = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      //查看任务执行的地方
      (hostname,threadName,Rules.rulesMap.getOrElse(word,"no found"),Rules.toString)
    })

    r.saveAsTextFile(args(1))

    sc.stop()
  }
}
