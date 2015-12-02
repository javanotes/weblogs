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
import java.util.UUID;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

import com.ericsson.weblogs.domain.annot.CustomIndexField;
import com.ericsson.weblogs.domain.annot.CustomIndexOption;
import com.ericsson.weblogs.domain.annot.CustomIndexSchema;
import com.ericsson.weblogs.domain.annot.CustomIndexed;
import com.ericsson.weblogs.domain.annot.FullTextSearchable;
import com.ericsson.weblogs.domain.annot.LuceneIndex;
import com.ericsson.weblogs.dto.LogRequest;

import lombok.Getter;
import lombok.Setter;

@Table(value = "data_points")
@CustomIndexed(className = "com.stratio.cassandra.lucene.Index", option = @CustomIndexOption(schema = @CustomIndexSchema(fields = 
{@CustomIndexField(field = "log_text", type = "text"), @CustomIndexField(field = "event_ts", type = "uuid", sorted = true)})))
public class LogEvent implements Serializable{

  
  @Override
  public String toString() {
    return "LogEvent [id=" + id + "]";
  }
  public LogEvent()
  {
    
  }
  public LogEvent(UUID timestamp)
  {
    this();
    setId(new LogEventKey());
    getId().setTimestamp(timestamp);
  }
  public LogEvent(LogRequest req)
  {
    setId(new LogEventKey());
    getId().setAppId(req.getApplicationId());
    getId().setLevel(req.getLevel());
    setLogText(req.getLogText());
  }
  
  /**
   * 
   */
  private static final long serialVersionUID = 6486945252184335817L;
  @PrimaryKey@Getter@Setter
  private LogEventKey id;
  
  @FullTextSearchable@Column(value = "log_text")@Getter@Setter
  private String logText;
  
  @Column(value = "lucene")@Getter@Setter@LuceneIndex
  private String lucene;
  
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
