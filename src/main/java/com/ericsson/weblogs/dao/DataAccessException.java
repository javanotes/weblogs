/* ============================================================================
*
* FILE: DataAccessException.java
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

import lombok.Getter;
import lombok.Setter;

public class DataAccessException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public DataAccessException() {
    
  }

  @Getter@Setter
  private boolean didNotify;
  @Getter
  private boolean hasInitCause;
  @Override
  public Throwable initCause(Throwable e)
  {
    hasInitCause = true;
    return super.initCause(e);
  }
  public DataAccessException(String message) {
    super(message);
    
  }

  public DataAccessException(Throwable cause) {
    super(cause);
    
  }

  public DataAccessException(String message, Throwable cause) {
    super(message, cause);
    
  }
  @Override
  public String toString() {
    return "DataAccessException [Message()=" + getMessage() + "]";
  }

}
