/* ============================================================================
*
* FILE: ContainsAND.java
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

class ContainsAND extends ContainsClause {

  void embedOR(ContainsOR or)
  {
    embedded = or;
  }
  @Override
  public String toString()
  {
    StringBuilder s = new StringBuilder("(");
    for(String o : operands)
    {
      s.append(" contains '").append(o).append("' and");
    }
    if(s.toString().endsWith("and"))
    {
      s.delete(s.lastIndexOf("and"), s.length());
    }
    if(embedded != null)
    {
      s.append(" and (").append(embedded.toString()).append(")");
    }
    s.append(")");
    return s.toString();
    
  }
}
