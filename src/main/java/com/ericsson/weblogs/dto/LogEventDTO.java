/* ============================================================================
*
* FILE: LogEventDTO.java
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

import java.util.Date;

import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.weblogs.domain.LogEvent;

import lombok.Data;

@Data
public class LogEventDTO {

  public LogEventDTO(LogEvent domain) {

    this(domain.getId().getAppId(), 
        domain.getLogText(),
        new Date(UUIDs.unixTimestamp(domain.getId().getTimestamp())));
  }

  public LogEventDTO() {
    super();
  }

  public LogEventDTO(String applicationId, String logText,
      Date timestamp) {
    super();
    this.applicationId = applicationId;
    this.logText = logText;
    this.timestamp = timestamp;
  }

  private String applicationId, logText;
  private Date timestamp;
}