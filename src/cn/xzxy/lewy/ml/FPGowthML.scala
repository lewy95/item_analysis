package cn.xzxy.lewy.ml

import cn.xzxy.lewy.util.MysqlTrait
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object FPGowthML extends MysqlTrait {

  /**
    * 处理试题得分数据，制作成对错分析表
    *
    * @param tempMAQ   暂时记录每大题的小题的得分和数目
    * @param akinds    大题总数
    * @param okinds    客观题总数
    * @param paperCode 试卷编号
    * @param spark     sparkSession
    */
  def itemIfTrueFunc(paperCode: String, createTime: String, tempMAQ: ArrayBuffer[Int],
                     akinds: Int, okinds: Int, spark: SparkSession): Unit = {

    var tfKindNum = 1 //大题序号
    var tfItemIndex = 1 //小题序号
    var tfKqStart = tempMAQ(tfItemIndex - 1)
    var tfKqEnd = tempMAQ(tfItemIndex)
    var tfSql: String = ""

    while (tfKindNum <= akinds) {

      if (tfKindNum <= okinds) {
        for (it <- (tfKqStart + 1) to tfKqEnd) {
          tfSql += "if(item" + tfKindNum + ".i" + it + " > 0, \'" + it + "t\',\'" + it + "f\') i" + it + ","
        }
      } else {
        val tfSmark = tempMAQ(tfKindNum + (tempMAQ.length - 2) / 2)
        val tfLine = tfSmark / 2.0
        for (it <- (tfKqStart + 1) to tfKqEnd) {
          tfSql += "case when item" + tfKindNum + ".i" + it + " > " + tfLine + " then \'" + it + "t\' else \'" + it + "f\' end i" + it + ","
        }
      }

      tfKqStart += tempMAQ(tfItemIndex)

      tfKindNum += 1
      tfItemIndex += 1

      tfKqEnd += tempMAQ(tfItemIndex)
    }

    //去掉结尾多的一个逗号
    var tofDf = spark.sql("select " + tfSql.dropRight(1) + " from t_origin_item")

    import spark.implicits._

    //上面是所以试题的正误情况
    //现在需要过滤难度>0.8或难度<0.2的试题，避免对分析产生影响
    //找到难度不符合的试题序号
    val badItem = spark.sql("select * from t_item_index where paper_code = " + paperCode + " and create_time = " + createTime)
    //将不符合列放入一个集合中
    val badItemNum = badItem.select($"item_id").where($"nandu"> 0.8 or $"nandu"< 0.2)
      .rdd.map(_.toString().drop(1).dropRight(1)).collect()
    //删除不符合的列
    for(i <- badItemNum.indices) tofDf = tofDf.drop("i" + badItemNum(i))

    //下面是写入hdfs的代码
    //val splitRex:String = " "
    //公司服务器需要hadoop的路径不太一样
    //我的hdfs://hadoop01:9000/itemML/
    //公司默认读的是hdfs路径的itemML/
    //val outputPath = "itemML/" + paperCode + "/relevance/input"
    //数据写入hdfs
    //saveToHdfs(tofDf, outputPath, splitRex, SaveMode.Append)

    //进行fpgrowth关联性分析
    doFpGrowth(tofDf, paperCode, createTime, spark)

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
    val minSupport = 0.6 //最小支持度，只能在0~1之间
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
        val preOrder = rule.antecedent.map(_.dropRight(1)).mkString(",")
        val sufOrder = rule.consequent(0).dropRight(1)
        val confidence = rule.confidence.toString
        //val ruleList:List[String] = preOrder::sufOrder::confidence::Nil
        ruleArray += Array(preOrder, sufOrder, confidence)
        //ruleArray.map(parts=>KnowLedge(parts(0),parts(1),parts(2)))
      })

    println("write after-fp-data into mysql:t_item_fpg now ....")
    //将分析出的规则转入为df，并存入mysql数据库
    val ruleDF = ruleArray.map(
      elem =>
        KnowLedge(elem(0), elem(1), elem(2).toDouble, paperCode, createTime)
    ).toDF()

    ruleDF.write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_item_fpg", prop)

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
