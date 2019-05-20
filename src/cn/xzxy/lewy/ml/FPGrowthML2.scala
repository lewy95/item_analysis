package cn.xzxy.lewy.ml

import cn.xzxy.lewy.ml.FPGrowthML.{KnowLedge, doFpGrowth, prop}
import cn.xzxy.lewy.util.MysqlTrait
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object FPGrowthML2 extends MysqlTrait {

  /**
    * 处理试题得分数据，制作成对错分析表
    *
    * @param tempMAQ   暂时记录每大题的小题的得分和数目
    * @param akinds    大题总数
    * @param paperCode 试卷编号
    * @param spark     sparkSession
    */
  def itemIfTrueFunc(paperCode: String, createTime:String, tempMAQ: ArrayBuffer[Int], akinds: Int,
                     spark: SparkSession): Unit = {

    import spark.implicits._

    //拿到试题和知识点的对应关系
    val ikDf = spark.read.jdbc("jdbc:mysql://hadoop01:3306/packmas","t_item_knowledge",prop).
      select("*").where($"paper_code" === paperCode)
      .drop("paper_code").drop("ik_id")

    //根据知识点对题目进行聚合，并按序排列
    val ikGroupDf = ikDf.groupBy("kl_id").agg(collect_set("item_id").as("itids")).orderBy("kl_id")

    //将聚合后的dataframe转为rdd
    val ikTuple = ikGroupDf.rdd.map(x=>(x.getInt(0),x.getSeq[Int](1).toList)).collect

    //准备原数据
    var iMarkPartSql: String = ""
    for (i <- 1 to akinds) {
      iMarkPartSql += "item" + i + ".*,"
    }
    val itemTempDf = spark.sql("select " + iMarkPartSql.dropRight(1) + " from t_origin_item")
    itemTempDf.createOrReplaceTempView("t_temp_ik")

    //按聚合结果对同一知识点的试题进行得分求和
    //为数据离散化提供依据
    var ikSql = ""
    for (i <- ikTuple){
      var ikSqlPart = ""
      for(j <- i._2) {
        ikSqlPart += "i" + j + " + "
      }
      ikSql += ikSqlPart.dropRight(3) + " as k" + i._1 + ", "
    }
    val kltsDf = spark.sql("select " + ikSql.dropRight(2) + " from t_temp_ik")
    kltsDf.createOrReplaceTempView("t_temp_klts")

    val ikmTSMap = new scala.collection.mutable.HashMap[String, Int]

    //计算每一个知识点下所有试题的理论满分
    //处理为Map格式 Map(63 -> 7, 51 -> 2, 12 -> 5, 653 -> 2, 542 -> 6,....)
    ikTuple.foreach(kl => {
      var perItemTS = 0
      for (i <- 0 until tempMAQ.length/2 -1) {
        val minKN = tempMAQ.take(i + 1).sum
        val maxKN = tempMAQ.take(i + 1 + 1).sum
        //perItemTS += (kl._2.filter(x => x > minKN && x<= maxKN).size) * tempMAQ(tempMAQ.length/2 + i)
        //seq.filter(p).size == seq.count(q)
        perItemTS += kl._2.count(x => x > minKN && x<= maxKN) * tempMAQ(tempMAQ.length/2 + i)
        ikmTSMap += (kl._1.toString -> perItemTS)
      }
    })

    //拿到知识点序号的集合
    val ikNums = ikTuple.map(x=> x._1.toString)

    //拼接进行数据离散化的sql语句
    //离散化的方法：去中间值，高于中间值为h，低于中间值为l
    var iktsSql = ""
    for (i <- ikNums) {
      val sepLine = ikmTSMap(i)/2.0
      iktsSql += "case when k" + i + " >= " + sepLine + " then \'" + i + "h\' when k" + i + " < " + sepLine + " then \'" + i + "l\' end k" + i + ", "
    }
    //进行离散化
    val ikOverDf = spark.sql("select " + iktsSql.dropRight(2) + " from t_temp_klts")

    //进行fpgrowth关联性分析
    doFpGrowth(ikOverDf, paperCode, createTime, spark)

  }

  def doFpGrowth(tofDf: DataFrame, paperCode: String, createTime: String,
                 spark: SparkSession): Unit = {

    import spark.implicits._

    //val transactions = tofDf.rdd.map(_.toString.drop(1).dropRight(1).split(","))
    val transactions = tofDf.rdd.map(_.toSeq.toArray).map(
      line => line.map(_.toString)
    )
    //缓存一下，因为fpgrowth需要扫描两次原始事务记录
    transactions.cache()

    //设置参数
    val minSupport = 0.65 //最小支持度，只能在0~1之间
    val minConfidence = 0.85 //最小置信度，只能在0~1之间
    val numPartitions = 2 //数据分区

    println("Spark FP-Growth starts ....")

    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport).setNumPartitions(numPartitions)

    //训练模型
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    //println("所有的频繁项集：")
    //model.freqItemsets 本质是一个数组：
    //Array({t}: 3, {t,x}: 3, {t,x,z}: 3, {t,z}: 3, {s}: 3, {s,t}: 2, ....}
    //foreach遍历每一项记为freqItemRecord
    //{t}: 3
    //{t,x}: 3
    //{t,x,z}: 3
    //{t,z}: 3
    //freqItemRecord.items 对应前面的{t,x,z}，本质也是一个数组Array(t,x,z)
    //freqItemRecord.freq 对应后面的3，表示该项出现的频次
    //    model.freqItemsets.collect().foreach(
    //      freqItemRecord => {
    //        println(freqItemRecord.items.mkString("[", ",", "]") + "," + freqItemRecord.freq)
    //      })

    //通过置信度筛选出推荐规则
    //自动产生rdd的格式为{q,z} => {t}: 1.0
    //antecedent表示前项，本质也是一个数组Array(q, z)，可以有一个或多个元素
    //consequent表示后项，本质也是一个数组Array(t)，但只可以有一个元素
    //confidence表示该条规则的置信度
    //需要注意的是：
    //1.所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //2.不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重
    //    model.generateAssociationRules(minConfidence).collect().foreach(rule => {
    //      println(rule.antecedent.mkString(",") + "-->" +
    //        rule.consequent.mkString(",") + "-->" + rule.confidence)
    //    })

    /**
      * 下面的代码是把规则写入到Mysql数据库中，以后使用来做推荐
      * 如果规则过多就把规则写入redis，这里就可以直接从内存中读取了
      */
    //新建一个可变数据，用来存放分析出的规则
    val ruleArray = new ArrayBuffer[Array[String]]()

    //本质一个rule是 Array(a,b) => Array(c) : 0.8
    model.generateAssociationRules(minConfidence).collect().foreach(
      rule => {
        val preOrders = rule.antecedent.map(_.dropRight(1))
        val preOrderMax = preOrders.max
        val sufOrder = rule.consequent(0).dropRight(1)
        val confidence = rule.confidence.toString
		    //过滤知识点前后顺序不合理的关联记录
        if (preOrderMax < sufOrder) {
           ruleArray += Array(preOrders.mkString(","), sufOrder, confidence)
        }
      })

    println("write after-fp-data into mysql:t_item_fpg now ....")
    //将分析出的规则转入为df，并存入mysql数据库
    val ruleDf = ruleArray.map(
      elem =>
        KnowLedge(elem(0), elem(1), elem(2).toDouble, paperCode, createTime)
    ).toDF()

    ruleDf.write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_item_fpg", prop)

    println("write mysql:t_item_fpg finished ....")
    //查看规则生成的数量
    //println(model.generateAssociationRules(minConfidence).collect().length)

    println("Spark FP-Growth finished .....")
  }
  
  /**
    * 样例类：知识点关联
    * @param pre_order 前序知识点
    * @param suf_order 后序知识点
    * @param confidence 置信度
    * @param paper_code 试卷编号
    * @param create_time 创建时间
    */
  case class KnowLedge(pre_order: String, suf_order: String, confidence: Double,
                       paper_code: String, create_time: String)
}
