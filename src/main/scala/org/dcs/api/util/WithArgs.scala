package org.dcs.api.util

object WithArgs {
  val TargetArgSep = "?"
  val TargetArgSepEscaped = "\\?"
  val ArgSep = "&"
  val ArgAssignSymbol = "="

  def apply(withArgsStr: String): WithArgs = {
    val targetArgStr = withArgsStr.split(TargetArgSepEscaped)
    val target = targetArgStr(0)
    val args = if(targetArgStr.size > 1) {
      val argStr = targetArgStr(1)
      argStr.split(ArgSep).map(_.split(ArgAssignSymbol))
        .map { a =>
          if(a.length == 2)
            (a(0), Some(a(1)))
          else
            (a(0), None)
        }.toMap
    } else Map[String, Option[String]]()
    new WithArgs(target, args)
  }

  def apply(target: String, args: List[(String,String)]): WithArgs = {
    new WithArgs(target, args.map(kv => (kv._1, Some(kv._2))).toMap)
  }
}

case class WithArgs(target: String, args: Map[String, Option[String]]) {
  import WithArgs._

  def get(argKey: String): String = {
    val value = args.get(argKey)
    if(value.isDefined)
      if(value.get.isDefined)
        value.get.get
      else
        throw new IllegalArgumentException("Argument key " + argKey + " exists, but has no value")
    else
      throw new IllegalArgumentException("No value of argument key " + argKey)
  }

  def exists(argKey: String): Boolean = {
    args.exists(_._1 == argKey)
  }

  def contains(argKey: String): Option[String] = {
    args.get(argKey).flatten
  }

  override def toString(): String = {
    return target +
      (if(args.nonEmpty)
        TargetArgSep + args.map { a =>
          a._1 +
            (
              if(a._2.isDefined)
                ArgAssignSymbol + a._2.get
              else
                ""
              )
        }.mkString(ArgSep)
      else
        "")
  }
}
