/* ============================================================================
*
* FILE: LogRequests.java
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

import org.hibernate.validator.constraints.NotEmpty;

import lombok.Data;

@Data
public class LogRequests {

  public LogRequests() {
    super();
  }

  public LogRequests(List<LogRequest> batch) {
    super();
    this.batch.addAll(batch);
  }

  @NotEmpty
  private List<LogRequest> batch = new ArrayList<>();
}
