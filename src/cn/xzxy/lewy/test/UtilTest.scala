package cn.xzxy.lewy.test

import java.text.SimpleDateFormat
import java.util.Date

object UtilTest {

  def main(args: Array[String]): Unit = {
    println(nowDate())
  }

  def nowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(now)
    date
  }

}
