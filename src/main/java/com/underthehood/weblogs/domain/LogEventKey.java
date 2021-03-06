/* ============================================================================
*
* FILE: LogEventKey.java
*
* MODULE DESCRIPTION:
* See class description
*
* Copyright (C) 2015 by
* 
*
* The program may be used and/or copied only with the written
* permission from  or in accordance with
* the terms and conditions stipulated in the agreement/contract
* under which the program has been supplied.
*
* All rights reserved
*
* ============================================================================
*/
package com.underthehood.weblogs.domain;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;

import com.underthehood.weblogs.utils.CommonHelper;
import com.underthehood.weblogs.utils.TimeuuidGenerator;

import lombok.Data;
@Data
@PrimaryKeyClass
public class LogEventKey implements Serializable{

  LogEventKey()
  {
    setTimestamp(CommonHelper.makeTimeUuid());
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
  /**
   * Gets the type 1 UUID as a {@link java.util.Date} 
   * @return
   */
  public Date getTimestampAsDate()
  {
    return new Date(getTimestampAsLong());
  }
  /**
   * Gets the type 1 UUID as a timestamp in Java - EPOCH (Jan 1 1970..)
   * @return
   */
  public long getTimestampAsLong()
  {
    return TimeuuidGenerator.unixTimestamp(timestamp);
  }
  @Override
  public String toString() {
    return "[appId=" + appId + ", timestamp=" + getTimestampAsDate() + "]";
  }
  public void setTimestamp(UUID u)
  {
    this.timestamp = u;
    bucket = new SimpleDateFormat(CommonHelper.TIMEBUCKET_DATEFORMAT).format(getTimestampAsDate());
  }
}
