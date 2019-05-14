package cn.xzxy.lewy.test

import java.text.SimpleDateFormat
import java.util.Date

object UtilTest {

  def main(args: Array[String]): Unit = {
    //println(nowDate())

    val dowhat = "2"
    if (dowhat.contains("1")) println("doItemIndex")
    if (dowhat.contains("2")) println("doFPGrowth")
    if (dowhat.contains("3")) println("doKmeans")
  }

  def nowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(now)
    date
  }

}
