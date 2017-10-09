package org.dcs.api

import org.dcs.api.util.WithArgs

class UtilSpec extends ApiUnitFlatSpec {

  "WithArgs de / ser" should "be consistent" in {
    val Target = "org.dcs.api.SomeClass"
    val Arg1 = "arg1"
    val Val1 = "val1"
    val Arg2 = "arg2"
    val Val2 = "val2"
    val Arg3 = "arg3"

    val withArgsStr = Target + WithArgs.TargetArgSep +
      Arg1 + WithArgs.ArgAssignSymbol + Val1 + WithArgs.ArgSep +
      Arg2 + WithArgs.ArgAssignSymbol + Val2 + WithArgs.ArgSep +
      Arg3

    assert(WithArgs(Target, Map(Arg1 -> Some(Val1), Arg2 -> Some(Val2), Arg3 -> None)).toString == withArgsStr)
    assert(WithArgs(Target, List()).toString == Target)
    assert(WithArgs(Target, List(Arg1 -> Val1)).get(Arg1) == Val1)
    intercept[IllegalArgumentException](
      WithArgs(Target, List(Arg1 -> Val1)).get(Arg2)
    )
    assert(WithArgs(Target, List(Arg1 -> Val1)).contains(Arg1).get == Val1)
    assert(WithArgs(Target, List(Arg1 -> Val1)).contains(Arg2).isEmpty)
    assert(WithArgs(Target, Map(Arg1 -> Some(Val1), Arg2 -> Some(Val2), Arg3 -> None)).exists(Arg3))
  }

}
