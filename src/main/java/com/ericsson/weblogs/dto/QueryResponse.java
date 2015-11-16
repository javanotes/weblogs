/* ============================================================================
*
* FILE: QueryResponse.java
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
package com.ericsson.weblogs.dto;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class QueryResponse {

  private List<LogEventDTO> logs = new ArrayList<>();

  public QueryResponse(List<LogEventDTO> logs) {
    super();
    this.logs.addAll(logs);
  }

  public QueryResponse() {
    super();
  }
  
}
