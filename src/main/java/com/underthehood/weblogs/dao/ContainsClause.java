/* ============================================================================
*
* FILE: ContainsClause.java
*
* MODULE DESCRIPTION:
* See class description
*
* Copyright (C) 2015 by
* 
*
* The program may be used and/or copied only with the written
* permission from  or in accordance with
* the terms and conditions stipulated in the agreement/contract
* under which the program has been supplied.
*
* All rights reserved
*
* ============================================================================
*/
package com.underthehood.weblogs.dao;

import java.util.ArrayList;
import java.util.List;

abstract class ContainsClause {

  protected final List<String> operands = new ArrayList<>();
  protected ContainsClause embedded;
  void addOperand(String opand)
  {
    operands.add(opand);
  }
  void addOperands(List<String> opands)
  {
    operands.addAll(opands);
  }
}
