package JDBCRdd

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRddDemo {

  val getConn = () => {
    DriverManager.getConnection("jdbc:mysql://116.56.140.131:13306/sparktest?characterEncoding=UTF-8", "root", "root")
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //创建RDD，这个RDD会记录以后从MySQL中读数据
    //new 了RDD，里面没有真正要计算的数据，而是告诉这个RDD，以后触发Action时到哪里读取数据
    val jdbcRDD: RDD[(Int, String, Int)] = new JdbcRDD(
      sc,
      getConn,
      "SELECT * FROM user WHERE id >= ? AND id < ?",
      1,
      5,
      2, //分区数量,分区数量会影响查询结果,由于查询条件也被切分了
      rs => {
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      }
    )

    //触发Action
    val r = jdbcRDD.collect()
    println(r.toBuffer)
    sc.stop()
  }
}