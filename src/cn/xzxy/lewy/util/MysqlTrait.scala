package cn.xzxy.lewy.util

import java.util.Properties

trait MysqlTrait {

  //存放mysql配置
  val prop = new Properties
  prop.setProperty("user", "root")
  prop.setProperty("password", "123456")

}
