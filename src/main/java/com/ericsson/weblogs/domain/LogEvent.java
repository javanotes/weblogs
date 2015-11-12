/* ============================================================================
*
* FILE: LogEvent.java
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
package com.ericsson.weblogs.domain;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.Indexed;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

import com.ericsson.weblogs.dto.LogRequest;

import lombok.Data;

@Table(value = "data_points")
@Data
public class LogEvent implements Serializable{

  
  public LogEvent()
  {
    
  }
  public LogEvent(LogRequest req)
  {
    setId(new LogEventKey());
    getId().setAppId(req.getApplicationId());
    getId().setBucket(req.getBucket());
    setLogText(req.getLogText());
  }
  
  /**
   * 
   */
  private static final long serialVersionUID = 6486945252184335817L;
  @PrimaryKey
  private LogEventKey id;
  @Column(value = "log_text")
  private String logText;
  @Column(value = "tokens")
  @Indexed
  private Set<String> tokens = new HashSet<>();
}
