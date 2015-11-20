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
import java.util.Date;
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
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, name = "row_id")
  private UUID rownum;
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 2, name = "event_ts", ordering = Ordering.DESCENDING)
  private Date timestamp;
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    LogEventKey other = (LogEventKey) obj;
    if (appId == null) {
      if (other.appId != null)
        return false;
    } else if (!appId.equals(other.appId))
      return false;
    if (rownum == null) {
      if (other.rownum != null)
        return false;
    } else if (!rownum.equals(other.rownum))
      return false;
    if (timestamp == null) {
      if (other.timestamp != null)
        return false;
    } else if (!timestamp.equals(other.timestamp))
      return false;
    return true;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((appId == null) ? 0 : appId.hashCode());
    result = prime * result + ((rownum == null) ? 0 : rownum.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }
  
}
