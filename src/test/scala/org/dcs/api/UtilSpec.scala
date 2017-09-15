package org.dcs.api

import org.dcs.api.util.WithArgs

class UtilSpec extends ApiUnitFlatSpec {

  "WithArgs de / ser" should "be consistent" in {
    val Target = "org.dcs.api.SomeClass"
    val Arg1 = "arg1"
    val Val1 = "val1"
    val Arg2 = "arg2"
    val Val2 = "val2"

    val withArgsStr = Target + WithArgs.TargetArgSep +
      Arg1 + WithArgs.ArgAssignSymbol + Val1 + WithArgs.ArgSep +
      Arg2 + WithArgs.ArgAssignSymbol + Val2

    assert(WithArgs(Target, Map(Arg1 -> Val1, Arg2 -> Val2)).toString == withArgsStr)
    assert(WithArgs(Target, Map()).toString == Target)
  }

}
