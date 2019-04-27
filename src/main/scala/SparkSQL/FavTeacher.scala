package SparkSQL

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    val topN = 2

    val spark = SparkSession.builder().appName("FavTeacher").master("local[4]").getOrCreate()

    val lines : Dataset[String] = spark.read.textFile("E:\\SparkLearn\\src\\main\\resources\\teacher.log")

    import spark.implicits._
    val df : DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost
      //学科的index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)
      (subject, teacher)
    }).toDF("subject","teacher")

    df.createTempView("v_teacher")

    //某个学科下的某个老师的访问次数
    val temp1 : DataFrame = spark.sql("select subject,teacher,count(*) as count from v_teacher group by subject,teacher")
    temp1.createTempView("v_temp_subject_teacher_count")

    //按学科分组，按照count排序
    val temp2 : DataFrame = spark.sql("select subject,teacher,row_number() over(partition by subject order by count) sub_rk,rank() over(order by count desc) g_rk from v_temp_subject_teacher_count")
    temp2.createTempView("v_temp_group_rank")

    temp2.show()

    val temp3 : DataFrame = spark.sql(s"select subject,teacher from v_temp_group_rank where sub_rk <= $topN")

    temp3.show()

    spark.stop()
  }
}
