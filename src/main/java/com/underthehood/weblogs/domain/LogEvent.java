/* ============================================================================
*
* FILE: LogEvent.java
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
import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.util.Assert;

import com.underthehood.weblogs.domain.annot.CustomIndexField;
import com.underthehood.weblogs.domain.annot.CustomIndexOption;
import com.underthehood.weblogs.domain.annot.CustomIndexSchema;
import com.underthehood.weblogs.domain.annot.CustomIndexed;
import com.underthehood.weblogs.domain.annot.FullTextSearchable;
import com.underthehood.weblogs.domain.annot.LuceneIndex;
import com.underthehood.weblogs.domain.annot.SearchIndexed;
import com.underthehood.weblogs.dto.LogRequest;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.Getter;
import lombok.Setter;

@Table(value = "data_points")
@CustomIndexed(className = "com.stratio.cassandra.lucene.Index", option = @CustomIndexOption(schema = @CustomIndexSchema(fields = {
    @CustomIndexField(field = "log_text", type = "text"),
    @CustomIndexField(field = "event_ts", type = "uuid",  sorted = true),
    @CustomIndexField(field = "level", type = "string") }) ) )
public class LogEvent implements Serializable{

  
  @Override
  public String toString() {
    return "LogEvent [id=" + id + "]";
  }
  public LogEvent()
  {
    setId(new LogEventKey());
  }
  public LogEvent(UUID timestamp)
  {
    this();
    getId().setTimestamp(timestamp);
  }
  public LogEvent(LogRequest req)
  {
    this();
    getId().setAppId(req.getApplicationId());
    setLevel(req.getLevel());
    setLogText(req.getLogText());
    setExecId(req.getExecutionId());
    getId().setTimestamp(CommonHelper.makeTimeUuid(req.getTimestamp()));
    Assert.isTrue(req.getTimestamp() == getId().getTimestampAsLong(), "Timestamp -> TimeUUID conversion incorrect");
  }
  
  /**
   * 
   */
  private static final long serialVersionUID = 6486945252184335817L;
  @PrimaryKey@Getter@Setter
  private LogEventKey id;
  
  @FullTextSearchable@Column(value = "log_text")@Getter@Setter
  private String logText;
  
  @Column(value = "level")@Getter@Setter@SearchIndexed(ordinal = 0)
  private String level = "INFO";
  
  @Column(value = "lucene")@Getter@Setter@LuceneIndex
  private String lucene;
  @Column(value = "exec_id")@Getter@Setter  
  private String execId;
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    LogEvent other = (LogEvent) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    return true;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    return result;
  }
}
