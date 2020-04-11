package com.scala.scalastyle

import scala.tools.jline_embedded.internal.TestAccessible

/**
  * author wang.long
  * date 2019/12/7 10:54
  * Description 改变新的scala
  * Mail wang.long@ustcinfo.com
  */
object ifElse {
  def main(args: Array[String]): Unit = {

    /**
      * ifelse的广度和深度
      * if(....){
      * }
      * else if(...){
      * }
      * else if(...){
      * }
      * else if(....){
      * }
      * else if(....){
      * }
      * else if(....){
      * }
      */
    /**
      * 保证每个if/else 块代码精简
      * 不要嵌套if/else
      * 不要使用过多的分支
      * 不用if/else
      */
    /**
      * 使用option来避免null值
      */
//    var bj = ""
//    jackOpt.map(item=> bj=item)
//    //当然如果你需要默认值的话
//    jackOpt.map(item=> bj=item).getOrElse("")
    /**
      * val startingOffsets = (parameters.get("startingOffsets") match {
      * case Some(value) => Option(value)
      * case None =>
      * (parameters.get("binlogIndex"), parameters.get("binlogFileOffset")) match {
      * case (Some(index), Some(pos)) => Option(BinlogOffset(index.toLong, pos.toLong).offset.toString)
      * case (Some(index), None) => Option(BinlogOffset(index.toLong, 4).offset.toString)
      * case _ => None
      * }
      * }).map(f => LongOffset(f.toLong))
      *
      *     parameters.get("startingOffsets").map(f => LongOffset(f.toLong))
      * 如果你使用Map的get方法，天然得到的就是一个Option,所以我们可以先进行Match操作，然后如果为None,我还可以尝试找到一些其他的候选值，如果后选值也没有，还是返回None,最后如果有值的话，统一包裹成LongOffset. 我们看第一段代码的好处非常明显，在我们日常中，我们总是可以从多个地方去拿值，而且有优先顺序，我们拿到的值最好都是Raw值，最后得到了之后统一处理，否则我们需要保证每一个分支都没有忘记用LongOffset包裹。
      *
      * 所以Option是消解if/else 中null值判断的一个好技巧。另外我们也看到了 match本质上和if/else一样，都是分支流程：
      */
    /**
      * (parameters.get("binlogIndex"), parameters.get("binlogFileOffset")) match {
      * case (Some(index), Some(pos)) => Option(BinlogOffset(index.toLong, pos.toLong).offset.toString)
      * case (Some(index), None) => Option(BinlogOffset(index.toLong, 4).offset.toString)
      * case _ => None
      * }
      */
  }
}
