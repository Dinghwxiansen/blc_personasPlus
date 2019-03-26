package cn.com.snsoft.user_tags

/**
  *
  * 定义特质，打标签的方法
  * 约束，特定的返回类型
  * 可以传任意类型的参数
  * 返回类型为map[String,string]
  *
  * 作用范围为用户基本属性标签,用户登录日志处理
  */
trait tags {
  def mkTags(args: Any*): Map[String,String]

}
