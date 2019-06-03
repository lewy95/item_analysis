package cn.xzxy.lewy.etl

import java.io.File

import cn.xzxy.lewy.ml.{FPGrowthML, FPGrowthML2, KmeansML}
import cn.xzxy.lewy.util.{HdfsTrait, MysqlTrait}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable._

object ItemETLPro extends MysqlTrait with HdfsTrait {

  def main(args: Array[String]): Unit = {

    //关闭不必要的日志
    //Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConf = new SparkConf()
      //.setAppName("itemAnalysis")
      //.setMaster("local[*]")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.testing.memory", "1073741824")
      .set("spark.sql.warehouse.dir", "hdfs://hadoop01:9000/spark-warehouse")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    //val sc = new SparkContext(sparkConf)

    //判断传入的参数个数是否正确
    if(args.length != 4){
      System.err.println("Usage: <inputDir> <paperCode> <submitTime> <executorStep>")
      System.exit(-1)
    }

    //接收命令行传递的参数
    val input = args(0) //输入文件目录hdfs://hadoop01:9000/itemdata/reportTime=2019-02-27 这个路径由web端生成

    val paperCode: String = args(1) //试卷编号 这个也由web端生成
    val submitTime: String = args(2) //用户提交时间 这个也由web端生成

    //设置执行过程
    //  1:只执行试卷指标分析，其他都不执行
    //  2:只执行关联性分析，不执行聚类分析
    //  3:只执行聚类分析，不执行关联性分析
    //  一共有七种组合 3 + 3 + 1
    //  只做一个part的 1  2  3
    //  只做两个part的 12 12 23
    //  三个part都做的 123
    val executorStep = args(3) //由web端生成

    //获取时间戳最近的数据作为分析数据
    val path = input + "/" + paperCode
    //根据提交时间来获取flume中对应的时间戳
    //但由于网络延迟，提交的时间戳和flume中不会一致，但肯定比它小，所以取第一个比提交时间大的时间戳
    val createTime = getFiles(path).map(_.toString.split("\\.")(1)).filter(x=> x>= submitTime)(0)
    //这里还可以优化，模式匹配+option判断createTime是否有值
    //没有值的话则取hdfs中最新的，有值的话直接用
    //可以解决处理试题指标后再想要进行相关性分析或聚类分析的操作

    //sparkSql原生api，需要进行隐式转换
    import spark.implicits._

    /**
      * part 1 : 数据初始化
      * jobs:
      * 1. 导入原始数据
      * 2. 创建数据库即数据库表
      * 3. 拿到试题的信息（各大题题数及分值）
      */
    //导入试题得分信息
    val originItemDf = spark.read.json(path + "/data." + createTime)
    //注册为临时表
    originItemDf.createOrReplaceTempView("t_origin_item")

    //从数据库中读取试卷信息
    val originPaperDf = spark.read.jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_paper_item", prop)
      .select("*").where($"paper_code" === paperCode)
    originPaperDf.createOrReplaceTempView("t_origin_paper")

    //选择目标数据库，若不存在则创建
    //当数据库已存在，再执行下面这句话会报错，可能是版本的问题，不影响
    spark.sql("create database if not EXISTS packmas_fcb")
    spark.sql("use packmas_fcb")

    //先拿到大题的数目（总题数，客观题题数，主观题题数）
    val akinds = originPaperDf.select($"all_items").first.getAs[Int]("all_items")
    lazy val okinds = originPaperDf.select($"o_items").first.getAs[Int]("o_items")
    lazy val skinds = originPaperDf.select($"s_items").first.getAs[Int]("s_items")

    //拼接查询试题表的sql
    val maqSql = getMAQSql(paperCode, akinds)
    //创建一个空的ArrayBuffer，存放各大题的题数和分值
    val markAndQuantity = ArrayBuffer[Int]()

    val paperRow = spark.sql(maqSql).collect()(0)
    //将row中数据，添加进变成数组中
    for (i <- 0 until akinds * 2) markAndQuantity += paperRow.getInt(i)

    //往临时变长数组的左右两边各添加一个元素，为下面计算提供给方便
    val emptyArray = ArrayBuffer[Int]()
    val tempMAQ = emptyArray ++ markAndQuantity
    tempMAQ.insert(0, 0)
    tempMAQ += 0

    /**
      * part 2 :试题基本指标分析
      */
    if (executorStep.contains("1")) {

      println("totalScore starts ....")

      //拿到拼接后的sql，计算试题的总得分情况
      val totalScoreDf = getTotalScoreDf(tempMAQ, akinds, paperCode, submitTime, spark)

      //记录试题样本总数
      val itemCount = originItemDf.count().toInt

      //确定高低分组的人数（总样本数*0.27）
      val perGroupCount: Int = (itemCount * 0.27).round.toInt

      //分别获得高分组和低分组学生信息
      val hgDf = totalScoreDf.orderBy($"totalScore".desc).limit(perGroupCount)
      hgDf.createOrReplaceTempView("t_item_hgInfo")
      val lgDf = totalScoreDf.orderBy($"totalScore").limit(perGroupCount)
      lgDf.createOrReplaceTempView("t_item_lgInfo")

      /**
        * step2: 计算每道试题的指标并落地
        */
      println("itemIndex starts ....")
      val itemIndexDf = itemIndexFunc(paperCode, submitTime, itemCount, perGroupCount, okinds, skinds, tempMAQ, markAndQuantity, spark)

      /**
        * step3: 计算整张试卷的指标并落地
        */
      println("paperIndex starts ....")
      val paperIndexDf = paperIndexFunc(paperCode, submitTime, markAndQuantity, spark)

      //**************基于上面三步*****************
      //数据落地到mysql都写在一起
      println("now write into mysql:t_score_total starts ....")
      totalScoreDf.selectExpr("stuCode as stu_code", "totalScore as total_score", "paperCode as paper_code", "create_time")
        .write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_score_total", prop)

      println("now write into mysql:t_item_index starts ....")
      itemIndexDf.write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_item_index", prop)

      println("now write into mysql:t_paper_index starts ....")
      paperIndexDf.write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_paper_index", prop)
      //********到此试题基本指标分析完成************
    }

    /**
      * part 3 : 执行fpGrowth，进行关联性分析
      */
    if (executorStep.contains("2")) {
      //val relation = new FPGowthML
      //以试题为标准的关联性分析
      //FPGrowthML.itemIfTrueFunc(paperCode, createTime, tempMAQ, akinds, okinds, spark)
      //以知识点为标准的关联性分析
      FPGrowthML2.itemIfTrueFunc(paperCode, submitTime, tempMAQ, akinds, spark)
    }

    /**
      * part 4 : 执行kmeans，执行聚类分析
      */
    if (executorStep.contains("3")) {
      //val cluster = new KmeansML
      KmeansML.itemClusterFunc(paperCode, submitTime, akinds, tempMAQ, spark)
    }

    //关闭sparkSession
    spark.stop()

  }

  /**
    * 完成每道题的指标统计（建表，计算，落地）
    *
    * @param paperCode       试卷编号
    * @param createTime      创建时间
    * @param itemCount       试题总述
    * @param perGroupCount   分组后每组
    * @param okinds          客观题大题数
    * @param skinds          主观题大题数
    * @param tempMAQ         暂时记录每大题的小题的得分和数目
    * @param markAndQuantity 记录每大题的小题的得分和数目
    * @param spark           sparkSession
    */
  def itemIndexFunc(paperCode: String, createTime: String, itemCount: Int, perGroupCount: Int, okinds: Int, skinds: Int,
                    tempMAQ: ArrayBuffer[Int], markAndQuantity: ArrayBuffer[Int],
                    spark: SparkSession): DataFrame = {
    //在hive中创建每道试题的指标表 t_item_index，分区表

    spark.sql("create table if not exists t_item_index(" +
      "item_id int, " +
      "max_score int, " +
      "min_score int, " +
      "avg_score decimal(4,3), " +
      "fc_score decimal(4,3), " +
      "bzc_score decimal(4,3), " +
      "nandu decimal(4,3), " +
      "qufendu decimal(4,3)) " +
      "partitioned by(paper_code string,create_time string) " +
      "row format delimited fields terminated by ','")

    //计算每道试题的指标，并写入hive表中
    var oitemIndex = 1
    var kqStart = tempMAQ(oitemIndex - 1) //定义在外层
    var kqEnd = tempMAQ(oitemIndex) //定义在外层
    var oCalcSql: String = ""
    var sCalcSql: String = ""

    if (okinds > 0) {

      var okindNum = 1 //大题序号
      var omarkIndex = markAndQuantity.length / 2
      var omark = markAndQuantity(omarkIndex).toDouble

      while (okindNum <= okinds) {

        for (num <- (kqStart + 1) to kqEnd) {
          oCalcSql = "select " + num + " item_id, max(item" + okindNum + ".i" + num + ") max_score, min(item" + okindNum + ".i" + num + ") min_score, avg(item" + okindNum + ".i" + num + ") avg_score, VAR_POP(item" + okindNum + ".i" + num + ") fc_score, STDDEV_POP(item" + okindNum + ".i" + num + ") bzc_score, sum(item" + okindNum + ".i" + num + ")/" + omark + "/" + itemCount + " nandu, ((select sum(item" + okindNum + ".i" + num + ")/" + omark + "/" + perGroupCount + " from t_item_hgInfo) - (select sum(item" + okindNum + ".i" + num + ")/" + omark + "/" + perGroupCount + " from t_item_lgInfo)) qufendu, " + paperCode + " as paper_code, " + createTime + " as create_time from t_origin_item"
          val tempTableName = "temp_item_index_" + num
          spark.sql(oCalcSql).createOrReplaceTempView(tempTableName)
          spark.sql("insert into table t_item_index partition(paper_code=" + paperCode + ",create_time=" + createTime
            + ") select item_id, max_score, min_score, avg_score, fc_score, bzc_score, nandu, qufendu from " + tempTableName)
        }

        kqStart += tempMAQ(oitemIndex)
        //标记每答题得分的索引自增一
        omarkIndex += 1
        omark = markAndQuantity(omarkIndex).toDouble
        okindNum += 1
        oitemIndex += 1

        kqEnd += tempMAQ(oitemIndex)
      }
    }

    if (skinds > 0) {

      var skindNum = okinds + 1 //客观题结束后的下一题是主观题
      var sitemIndex = skindNum
      var smarkIndex = markAndQuantity.length / 2 + skindNum //其实是markAndQuantity.length/2 + okinds，只不过该索引是给tempMAQ用，而在tempMAQ前已经在一个元素
      var smark = tempMAQ(smarkIndex).toDouble

      while (skindNum <= skinds + okinds) {

        for (num <- (kqStart + 1) to kqEnd) {
          sCalcSql = "select " + num + " item_id, max(item" + skindNum + ".i" + num + ") max_score, min(item" + skindNum + ".i" + num + ") min_score, avg(item" + skindNum + ".i" + num + ") avg_score, VAR_POP(item" + skindNum + ".i" + num + ") fc_score, STDDEV_POP(item" + skindNum + ".i" + num + ") bzc_score, sum(item" + skindNum + ".i" + num + ")/" + smark + "/" + itemCount + " nandu, ((select sum(item" + skindNum + ".i" + num + ") from t_item_hgInfo)-(select sum(item" + skindNum + ".i" + num + ") from t_item_lgInfo))/" + perGroupCount + "/(max(item" + skindNum + ".i" + num + ") - min(item" + skindNum + ".i" + num + ")) qufendu, " + paperCode + " as paper_code, " + createTime + " as create_time from t_origin_item"
          val tempTableName = "temp_item_index_" + num
          spark.sql(sCalcSql).createOrReplaceTempView(tempTableName)
          spark.sql("insert into table t_item_index partition(paper_code=" + paperCode + ",create_time=" + createTime
            + ") select item_id, max_score, min_score, avg_score, fc_score, bzc_score, nandu, qufendu from " + tempTableName)
        }

        kqStart += tempMAQ(sitemIndex)
        smarkIndex += 1
        //标记每大题得分的索引自增一
        //上面有加了1,所以最后会越界
        //解决：给tempMAQ数组末尾再添一个元素
        smark = tempMAQ(smarkIndex).toDouble
        skindNum += 1
        sitemIndex += 1

        kqEnd += tempMAQ(sitemIndex)
      }
    }

    //拿到分区内排序后记录
    val itemIndexDf = spark.sql("select * from t_item_index where paper_code = " + paperCode + " and create_time = " + createTime + " order by item_id")

    itemIndexDf
  }

  /**
    * 完成整张试卷的指标统计（建表，计算，落地）
    *
    * @param paperCode       试卷编号
    * @param createTime      创建时间
    * @param markAndQuantity 记录每大题的小题的得分和数目
    * @param spark           sparkSession
    */
  def paperIndexFunc(paperCode: String, createTime: String, markAndQuantity: ArrayBuffer[Int], spark: SparkSession): DataFrame = {
    //创建试题总分的hive分区表：一级分区试卷编号，二级分区创建时间
    spark.sql("create table if not exists t_paper_index(" +
      "max_score int, " +
      "min_score int, " +
      "avg_score decimal(5,3), " +
      "fc_score decimal(7,3), " +
      "bzc_score decimal(5,3), " +
      "nandu decimal(5,3), " +
      "qufendu decimal(5,3), " +
      "xindu decimal(5,3)) " +
      "partitioned by(paper_code string, create_time string) " +
      "row format delimited fields terminated by ','")

    //拿到试题总数
    val iqAll = markAndQuantity.take(markAndQuantity.length / 2).sum.toDouble

    //拼接计算语句
    val tempIIDf = spark.sql("select avg(nandu) an, avg(qufendu) aq, sum(fc_score) sfc from t_item_index where paper_code = " + paperCode + " and create_time = " + createTime)
    tempIIDf.createOrReplaceTempView("temp_item_index")
    val paperSql = "select " + paperCode + " paper_code, max(total_score) max_score, min(total_score) min_score, avg(total_score) avg_score, VAR_POP(total_score) fc_score, STDDEV_POP(total_score) bzc_score, (select an from temp_item_index) nandu, (select aq from temp_item_index) qufendu, ((" + iqAll + "/" + (iqAll - 1) + ")*(1-(select sfc from temp_item_index)/var_pop(total_score))) xindu, " + createTime + " as create_time from t_score_total where paper_code = " + paperCode + " and create_time = " + createTime

    val paperIndexDf = spark.sql(paperSql)
    paperIndexDf.createOrReplaceTempView("temp_paper_index")
    //插入到hive表
    spark.sql("insert into table t_paper_index partition(paper_code=" + paperCode + ",create_time=" + createTime + ") select max_score, min_score, avg_score, fc_score, bzc_score, nandu, qufendu, xindu from temp_paper_index")

    paperIndexDf
  }

  /**
    * 计算每份试卷的总得分，并写入到hive表中
    *
    * @param tempMAQ    记录每大题的小题的得分和数目
    * @param akinds     大题总数
    * @param paperCode  试卷编号
    * @param createTime 创建时间
    * @param spark      sparkSession
    * @return
    */
  def getTotalScoreDf(tempMAQ: ArrayBuffer[Int], akinds: Int, paperCode: String,
                      createTime: String, spark: SparkSession): DataFrame = {
    //创建试题总分的hive分区表：一级分区试卷编号，二级分区创建时间
    spark.sql("create table if not exists t_score_total(" +
      "stu_code string, " +
      "total_score int) " +
      "partitioned by(paper_code string, create_time string) " +
      "row format delimited fields terminated by ','")

    var kindNum = 1 //大题序号
    var itemIndex = 1 //小题序号
    var itemKqStart = tempMAQ(itemIndex - 1)
    var itemKqEnd = tempMAQ(itemIndex)
    var totalScoreSql: String = ""
    while (kindNum <= akinds) {

      for (it <- (itemKqStart + 1) to itemKqEnd) {
        totalScoreSql += "item" + kindNum + ".i" + it + " + "
      }

      itemKqStart += tempMAQ(itemIndex)

      kindNum += 1
      itemIndex += 1

      itemKqEnd += tempMAQ(itemIndex)
    }

    totalScoreSql = totalScoreSql.dropRight(3) //删除右数开始的三个元素（索引从1开始）
    val tsSql = "select *, (" + totalScoreSql + ") totalScore, " + createTime + " as create_time from t_origin_item"

    val totalScoreDf = spark.sql(tsSql)
    //修改一下列的名称
    val tempTSDf = totalScoreDf.selectExpr("paperCode as paper_code", "stuCode as stu_code", "totalScore as total_score", "create_time")
    tempTSDf.createOrReplaceTempView("temp_score_total")

    //计算结果保存到hive分区表
    spark.sql("insert into table t_score_total partition(paper_code=" + paperCode + ",create_time=" + createTime + ") select stu_code, total_score from temp_score_total")

    totalScoreDf
  }

  /**
    * 拼接查询试题信息的sql
    *
    * @param paperCode 试卷编号
    * @param akinds    大题总数
    * @return
    */
  def getMAQSql(paperCode: String, akinds: Int): String = {
    //创建一个空的ArrayBuffer
    var kqSql = "" //拼接quantity相关
    var kmSql = "" //拼接mark相关
    for (ik <- 1 to akinds) {
      kqSql += "part" + ik + "_qty,"
      kmSql += "part" + ik + "_mark,"
    }
    //去掉最后多的","
    val maqPartSql = (kqSql + kmSql).dropRight(1)
    "select " + maqPartSql + " from t_origin_paper op where op.paper_code = " + paperCode
  }

}
