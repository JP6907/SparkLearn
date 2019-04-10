package TopN

object TestTeacherSpilt extends App {

  val lines = "http://bigdata.edu360.cn/laozhang";
  val splits :Array[String] = lines.split("/")
  val subject = splits(2).split("[.]")(0)
  val teacher = splits(3)

  println(subject)
  println(teacher)
}
