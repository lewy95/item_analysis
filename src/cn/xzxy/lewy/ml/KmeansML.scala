package cn.xzxy.lewy.ml

import cn.xzxy.lewy.util.MysqlTrait
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object KmeansML extends MysqlTrait {

  def itemClusterFunc(paperCode: String, createTime: String, akinds: Int, tempMAQ: ArrayBuffer[Int],
                      spark: SparkSession): Unit = {
    var iMarkPartSql: String = ""
    for (i <- 1 to akinds) {
      iMarkPartSql += "item" + i + ".*,"
    }
    val itemOriginDf = spark.sql("select stuCode," + iMarkPartSql.dropRight(1) + " from t_origin_item")
    itemOriginDf.createOrReplaceTempView("temp_item_mark")

    val itemMarkDf = spark.sql("select substr(stuCode,1,2) gc, " +
      "substr(stuCode,3,2) mc, " +
      "substr(stuCode,5,2) cc, " +
      "substr(stuCode,7,2) nc, " + iMarkPartSql.dropRight(1) + " from t_origin_item")

    doKmeans(itemMarkDf, paperCode, createTime, tempMAQ, spark)
  }

  def doKmeans(iMarkDf: DataFrame, paperCode: String, createTime: String,
               tmaq: ArrayBuffer[Int], spark: SparkSession): Unit = {

    /**
      * 第一部分：数据处理
      */
    val trainingData = iMarkDf.rdd.map(_.toSeq).map(line => {
      //将一行数据转化为一个密集矩阵（本质是一个double类型的数组）
      Vectors.dense(line.toArray.map(_.toString.toDouble))
    })
    //缓存一下原数据
    trainingData.cache()

    /**
      * 输入控制参数
      * 首先明确可以设定的参数
      * private var k: Int   聚类个数
      * private var maxIterations: Int   迭代次数
      * private var runs: Int,   运行kmeans算法的次数，spark2.0以后已废弃
      * private var initializationMode: String 初始化聚类中心的方法
      * 有两种选择 ① 默认方式 KMeans.K_MEANS_PARALLEL 即 KMeans|| ② KMeans.RANDOM 随机取
      * private var initializationSteps: Int 初始化步数
      * private var epsilon: Double  判断kmeans算法是否达到收敛的阈值 这个阈值由spark决定
      * private var seed: Long)private var k: Int 表示初始化时的随机种子
      */
    val numClusters: Int = 4 //簇的个数
    val numIterations = 20 //迭代次数
    var clusterIndex: Int = 0 //簇的索引

    /**
      * 第二部分：训练聚类模型
      * 参数一：被训练的数据集
      * 参数二：最后聚类的个数
      * 参数三：迭代次数
      * 还可以指定其他参数：initializationMode，initializationSteps，seed
      */
    val model: KMeansModel = KMeans.train(trainingData, numClusters, numIterations)

    //查看聚类的个数，即传入的k
    println("Spark MLlib K-means clustering starts ....")
    println("Cluster Number:" + model.clusterCenters.length)

    //println("Cluster Centers Information Overview:")
    //聚类中心点：
    //Center Point of Cluster 0:
    //[xxx,xxx,xxx,xxx,xxx,xxx,xxx,xxx]
    //Center Point of Cluster 1:
    //[xxx,xxx,xxx,xxx,xxx,xxx,xxx,xxx]
    //Center Point of Cluster 2:
    //[xxx,xxx,xxx,xxx,xxx,xxx,xxx,xxx]
    //Center Point of Cluster 3:
    //[xxx,xxx,xxx,xxx,xxx,xxx,xxx,xxx]
    /*
    model.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })*/

    /**
      * 第三部分：根据聚类结果开始检查每个点属于哪个簇
      * 注意：有时候训练数据和真正进行聚类的数据不同，只需要像上面一样再都进来处理一下即可
      */
    //创建一个可变数组，存放所有的聚类结果
    val resultArray = new ArrayBuffer[Array[Double]]()

    trainingData.collect().foreach(lineData => {
      val predictedClusterIndex: Int = model.predict(lineData)

      val lArray = lineData.toArray
      //val stuCode = lArray(0).toInt.toString + lArray(1).toInt.toString + lArray(2).toInt.toString + lArray(3).toInt.toString
      //拼接一个新数组，即在原数组中加入簇的序号
      val rArray = Array.concat(lArray, Array(predictedClusterIndex.toDouble))

      resultArray += rArray
    })

    //根据聚类结果（簇的编号进行分组），并将分好的每簇中的数据写进对应的临时表
    val cIndexOfLine = trainingData.first().size
    val resultMap = resultArray.groupBy(x => x(cIndexOfLine))

    //对每一簇中数据进行进一步分析
    minKmeansData(paperCode, createTime,0.0, tmaq, resultMap, spark)
    minKmeansData(paperCode, createTime,1.0, tmaq, resultMap, spark)
    minKmeansData(paperCode, createTime,2.0, tmaq, resultMap, spark)
    minKmeansData(paperCode, createTime,3.0, tmaq, resultMap, spark)

    println("Spark MLlib K-means clustering finished ....")
  }

  /**
    * 针对每个簇中数据进行进一步分析
    *
    * @param center    聚类中心
    * @param tmaq      存放试题得分和题数的临时数组
    * @param resultMap map形式的聚类结果
    * @param spark     sparkSession
    */
  def minKmeansData(paperCode: String, createTime: String, center: Double, tmaq: ArrayBuffer[Int],
                    resultMap: Map[Double, ArrayBuffer[Array[Double]]], spark:SparkSession): Unit = {

    import spark.implicits._

    //获得某个簇中学生学号
    val clusterSC = resultMap(center).map(
      line => {
        val yc = castToCode(line(0))
        val mc = castToCode(line(1))
        val cc = castToCode(line(2))
        val nc = castToCode(line(3))
        yc.append(mc).append(cc).append(nc).toString
      })

    var scStr = ""
    for (i <- clusterSC.indices) scStr += clusterSC(i) + ","

    val clusterInDf = spark.sql("select * from temp_item_mark where stuCode in (" + scStr.dropRight(1) + ")")
    clusterInDf.createOrReplaceTempView("temp_item_cluster")

    val tmaqM = tmaq.drop(tmaq.length / 2).dropRight(1).map(_.*(clusterSC.length))

    val tbName = "temp_item_cluster"
    var mi = 1
    var tmi = 0
    var furIndexStr = ""
    var startNo = tmaq(0)
    var endNo = tmaq(1)
    while (mi < 6) {
      for (i <- (startNo + 1) to endNo) {
        furIndexStr += "select " + i + " id, sum(i" + i + ")/" + tmaqM(tmi) + " avg from " + tbName + " union all "
      }
      mi += 1
      tmi += 1
      startNo = endNo
      endNo += tmaq(mi)
    }

    //获取所有试题的得分均值情况
    val avgMarkDf = spark.sql(furIndexStr.dropRight(10))

    //0.2以下的试题太难，基本都不对，没有考虑的必要，同理0.8以下的也一样
    val lowAvg = avgMarkDf.select($"id").where($"avg" between(0.2, 0.3)).orderBy($"id")
    val laItem = lowAvg.rdd.flatMap(_.toSeq).collect().mkString(",")
    val highAvg = avgMarkDf.select($"id").where($"avg" between(0.7, 0.8)).orderBy($"id")
    val haItem = highAvg.rdd.flatMap(_.toSeq).collect().mkString(",")

    val kRecordDf = spark.sql("select stuCode stu_code, \"" + haItem + "\" good_item, \"" + laItem + "\" bad_item, " + paperCode + " paper_code, " + center + " centers, " + createTime + " create_time from temp_item_cluster")

    //写入数据库中
    println("now center:" + center + " data write into mysql:t_item_cluster starts ....")
    kRecordDf.write.mode("append").jdbc("jdbc:mysql://hadoop01:3306/packmas", "t_item_cluster", prop)
  }

  /**
    * 判断原有编号是否只有一位，若只有一位则在其前面拼一个“0”
    *
    * @param oc 原有编号
    * @return
    */
  def castToCode(oc: Double): StringBuffer = {
    val partCode: StringBuffer = new StringBuffer(oc.toString.dropRight(2))
    if (oc >= 10.0) partCode else partCode.insert(0, "0")
  }

  /**
    * 根据关键字过滤第一行
    *
    * @param line 输入行
    * @return
    */
  //private def isColumnNameLine(line: String): Boolean = {
  //  if (line != null && line.contains("Channel")) true
  //  else false
  //}
}