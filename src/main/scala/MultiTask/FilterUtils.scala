package MultiTask

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat


//数据格式：
//1|2016年9月16日,星期五,23:02:41|192.168.1.102|那谁|武士|男|1|0|0/800000000
//class FilterUtils  with Serializable{
//  //如果object使用了成员变量，那么会出现线程安全问题，因为object是一个单例，多线程可以同时调用这个方法
//  val dateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
//
//  //根据第一个字段过滤出数据
//  def filterByType(fields : Array[String],tp : String) = {
//    val _tp = fields(0)
//    _tp == tp
//  }
//
//  //根据时间过滤出数据
//  def filterByTime(fields : Array[String],startTime:Long,endTime:Long) = {
//    val time = fields(1)
//    val timeLong = dateFormat.parse(time).getTime
//    timeLong >= startTime && timeLong <=endTime
//  }
//}

object FilterUtils {
  //如果object使用了成员变量，那么会出现线程安全问题，因为object是一个单例，多线程可以同时调用这个方法
  //val dateFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
  //注意jar包，不要导错了
  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  //根据第一个字段过滤出数据
  def filterByType(fields : Array[String],tp : String) = {
    val _tp = fields(0)
    _tp == tp
  }

  //根据时间过滤出数据
  def filterByTime(fields : Array[String],startTime:Long,endTime:Long) = {
    val time = fields(1)
    val timeLong = dateFormat.parse(time).getTime
    timeLong >= startTime && timeLong <=endTime
  }
}