package cn.xzxy.lewy.util

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

trait HdfsTrait {

  /**
    * 配置获取hdfs
    * @param path
    * @return
    */
  def getHdfs(path: String): FileSystem = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  /**
    * 获取指定目录下的文件和子目录
    * @param path
    * @return
    */
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**
    * 获取指定目录下文件
    * @param path
    * @return
    */
  def getFiles(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
  }

  /**
    * 获取指定目录下的子目录
    * @param path
    * @return
    */
  def getDirs(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory())
  }

  /**
    * 将数据写入hdfs
    *
    * @param df
    * @param pathDir
    * @param splitRex
    * @param saveMode
    */
  def saveToHdfs(df: DataFrame, pathDir: String, splitRex: String, saveMode: SaveMode): Unit = {
    val allColumnName: String = df.columns.mkString(",")
    val result: DataFrame = df.selectExpr(s"concat_ws('$splitRex',$allColumnName) as allColumn")
    result.write.mode(saveMode).text(pathDir)
  }

}
