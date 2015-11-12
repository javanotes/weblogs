/* ============================================================================
*
* FILE: LogEventKey.java
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
import java.util.UUID;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

import lombok.Data;
@Data
@PrimaryKeyClass
public class LogEventKey implements Serializable{

  public LogEventKey()
  {
    
  }
  /**
   * 
   */
  private static final long serialVersionUID = -8319593299678087621L;
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0, name = "app_id")
  private String appId;
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1, name = "bucket")
  private String bucket;
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 2, name = "event_ts", ordering = Ordering.DESCENDING)
  private UUID timestamp;

}