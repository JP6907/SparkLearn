package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JoinTest").setMaster("local[4]")
    val session = SparkSession.builder().config(conf).getOrCreate()

    //设置广播的表的大小阈值，默认为10M，-1为关闭广播，执行计划由BroadcastHashJoin变成SortMergeJoin
    //session.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    import session.implicits._
    val personDS: Dataset[String] = session.createDataset(List("1,zhou,China", "2,li,US", "3,hong,UK"))
    val personDF: DataFrame = personDS.map(line => {
      val fields = line.split(",")
      val id = fields(0)
      val name = fields(1)
      val nation = fields(2)
      (id, name, nation)
    }).toDF("id", "name", "nation")

    val nationDS: Dataset[String] = session.createDataset(List("China,中国", "US,美国"))
    val nationDF : DataFrame = nationDS.map(line => {
      val fields = line.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename,cname)
    }).toDF("ename","cname")

    /**
      * 第一种方式
      * 创建视图
      */
//    personDF.createTempView("v_person")
//    nationDF.createTempView("v_nation")
//
//    val result = session.sql("select name,cname from v_person join v_nation on nation = ename")

    /**
      * 第二种方式
      * DSL
      */
    val result = personDF.join(nationDF,$"nation"===$"ename","inner")

    //查看执行计划
    result.explain()

    result.show()

    session.stop()
  }
}
