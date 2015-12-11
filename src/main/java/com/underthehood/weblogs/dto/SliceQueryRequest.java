/* ============================================================================
*
* FILE: SliceQueryRequest.java
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
package com.underthehood.weblogs.dto;

import lombok.Getter;
import lombok.Setter;


public class SliceQueryRequest extends QueryRequest {

  @Getter@Setter
  private String searchTermEnd;
}
