/* ============================================================================
*
* FILE: ContainsOR.java
*
* MODULE DESCRIPTION:
* See class description
*
* Copyright (C) 2015 by
* ERICSSON
*
* The program may be used and/or copied only with the written
* permission from Ericsson Inc, or in accordance with
* the terms and conditions stipulated in the agreement/contract
* under which the program has been supplied.
*
* All rights reserved
*
* ============================================================================
*/
package com.ericsson.weblogs.dao;

class ContainsOR extends ContainsClause {

  void embedAND(ContainsAND and)
  {
    embedded = and;
  }
  @Override
  public String toString()
  {
    StringBuilder s = new StringBuilder("(");
    for(String o : operands)
    {
      s.append(" contains '").append(o).append("' or");
    }
    if(s.toString().endsWith("or"))
    {
      s.delete(s.lastIndexOf("or"), s.length());
    }
    if(embedded != null)
    {
      s.append(" or (").append(embedded.toString()).append(")");
    }
    s.append(")");
    return s.toString();
    
  }
}
