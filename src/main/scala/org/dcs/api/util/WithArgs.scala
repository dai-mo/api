package org.dcs.api.util

object WithArgs {
  val TargetArgSep = "\\?"
  val ArgSep = "&"
  val ArgAssignSymbol = "="

  def apply(withArgsStr: String): WithArgs = {
    val targetArgStr = withArgsStr.split(TargetArgSep)
    val target = targetArgStr(0)
    val args = if(targetArgStr.size > 1) {
      val argStr = targetArgStr(1)
      argStr.split(ArgSep).map(_.split(ArgAssignSymbol)).map(a => (a(0), a(1))).toMap
    } else Map[String, String]()
    new WithArgs(target, args)
  }
}

case class WithArgs(target: String, args: Map[String, String]) {
  import WithArgs._

  def get(argKey: String): String = {
    val value = args.get(argKey)
    if(value.isDefined)
      value.get
    else
      throw new IllegalArgumentException("No value of argument key " + argKey)
  }

  override def toString(): String = {
    return target +
      (if(args.nonEmpty)
        TargetArgSep + args.map(a => a._1 + ArgAssignSymbol + a._2).mkString(ArgSep)
      else
        "")
  }
}
