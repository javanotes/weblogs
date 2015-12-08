/* ============================================================================
*
* FILE: BatchDataAccessException.java
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
package com.underthehood.weblogs.dao;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;

public class BatchDataAccessException extends DataAccessException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public BatchDataAccessException() {
    super();
  }

  @Getter
  private final List<DataAccessException> exceptions = new ArrayList<>();
  public BatchDataAccessException(String message) {
    super(message);
    
  }
  @Override
  public String toString() {
    return "BatchDataAccessException [exceptions=" + exceptions + "]";
  }


}
