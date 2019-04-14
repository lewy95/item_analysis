package cn.xzxy.lewy.test

import org.apache.spark.sql.SparkSession

object ReadHdfsTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("itemIndexEtl")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val input = args(0) //hdfs://hadoop01:9000/itemdata/reportTime=2019-02-27/paperCode 这个路径由web端生成

    //******优化：可以添加partition数目
    val originItemDf = spark.read.json(input + "_i")
    //注册为临时表
    originItemDf.createOrReplaceTempView("t_origin_item")

    //导入试卷信息
    val originPaperDf = spark.read.json(input + "_p")
    originPaperDf.createOrReplaceTempView("t_origin_paper")

    spark.sql("select * from t_origin_item").show(10)
    spark.sql("select * from t_origin_paper").show

    spark.stop()
  }
}
