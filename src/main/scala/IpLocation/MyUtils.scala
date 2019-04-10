package IpLocation

import java.sql.{Connection, DriverManager}

import scala.io.{BufferedSource, Source}

object MyUtils {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      //原来是16进制，即8位二进制
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def data2Mysql(it:Iterator[(String,Int)]):Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获取一个jdbc连接
    val conn : Connection = DriverManager.getConnection("jdbc:mysql://hdp-00:3306/sparkdb?characterEncoding=UTF-8","root","root")
    //将数据通过connection写入到数据库
    val pstm = conn.prepareStatement("insert into access_log values (?,?)")
    //逐条写入数据
    it.foreach(tp=>{
      pstm.setString(1,tp._1)
      pstm.setInt(2,tp._2)
      pstm.executeUpdate()
    })
    //关闭数据库连接
    if(pstm!=null){
      pstm.close()
    }
    if(conn!=null){
      conn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val rules : Array[(Long,Long,String)] = readRules("/home/zjp/Documents/Sparklearn/src/main/resources/ip/ip.txt")
    //转化成十进制
    val ipNum = ip2Long("114.215.43.42")
    //查找
    val index = binarySearch(rules,ipNum)
    //根据索引到rules中查找对应的数据
    val tp = rules(index)
    val province = tp._3
    println(province)
  }
}
