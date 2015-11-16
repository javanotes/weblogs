/* ============================================================================
*
* FILE: LogEventFinderDAO.java
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
package com.ericsson.weblogs.dao;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.weblogs.domain.LogEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j@Repository
public class LogEventFinderDAO extends LogEventDAO{

  private String[] pkCols;
  private String timeuuidCol;
  private String tokenCol;
    
  static final String REPLACE_PATTERN = "@val";
  
  static final String CONTAINS = " contains '"+REPLACE_PATTERN+"'";
  static final String TS_AS_LONG = "unixTimestampOf("+REPLACE_PATTERN+")";
  static final String TS_AS_DATE = "dateOf("+REPLACE_PATTERN+")";
  static final String MAX_UUID = "maxTimeuuid('"+REPLACE_PATTERN+"')";
  static final String MIN_UUID = "minTimeuuid('"+REPLACE_PATTERN+"')";
  
  @PostConstruct
  private void init()
  {
    pkCols = new String[pkFields.size()];
    int i = 0;
    for(Field f : pkFields.values())
    {
      pkCols[i++] = f.getAnnotation(PrimaryKeyColumn.class).name();
    }
    
    StringBuilder s = new  StringBuilder("select ");
    for(Field f : allFields.values())
    {
      String c = f.isAnnotationPresent(PrimaryKeyColumn.class)
          ? f.getAnnotation(PrimaryKeyColumn.class).name()
          : f.getAnnotation(Column.class).value();
          
      s.append(c).append(",");
      if(f.getType() == UUID.class)
      {
        timeuuidCol = f.getAnnotation(PrimaryKeyColumn.class).name();
      }
      if(Set.class.isAssignableFrom(f.getType()))
      {
        tokenCol = f.getAnnotation(Column.class).value();
      }
    }
    
    s.deleteCharAt(s.lastIndexOf(","));
    s.append(" from ").append(table).append(" where ");
    
    log.debug(">>>>> Prepared dynamic projection: "+s);
    
    
  }
  
  
  public List<LogEvent> findByAppId(String appId)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where(QueryBuilder.eq(pkCols[0], appId));
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
   
  public List<LogEvent> findByAppIdContains(final String appId, String token)
  {
    Select sel = QueryBuilder.select().from(table);
    
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.contains(tokenCol, token));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
    
    
  }
  
    
  public List<LogEvent> findByAppIdBetweenDatesContains(final String appId, String token, final Date fromDate, final Date toDate)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.gt(timeuuidCol, UUIDs.endOf(fromDate.getTime())))
    .and(QueryBuilder.lt(timeuuidCol, UUIDs.startOf(toDate.getTime())))
    .and(QueryBuilder.contains(tokenCol, token));
        
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  
  public List<LogEvent> findByAppIdAfterDateContains(final String appId, String token, final Date fromDate)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.gt(timeuuidCol, UUIDs.endOf(fromDate.getTime())))
    .and(QueryBuilder.contains(tokenCol, token));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
    
  public List<LogEvent> findByAppIdBeforeDateContains(String appId, String token, final Date toDate)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.lt(timeuuidCol, UUIDs.startOf(toDate.getTime())))
    .and(QueryBuilder.contains(tokenCol, token));
        
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBetweenDates(String appId, Date fromDate, Date toDate)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.gt(timeuuidCol, UUIDs.endOf(fromDate.getTime())))
    .and(QueryBuilder.lt(timeuuidCol, UUIDs.startOf(toDate.getTime())));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBeforeDate(String appId, Date toDate)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.lt(timeuuidCol, UUIDs.startOf(toDate.getTime())));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdAfterDate(String appId, Date fromDate)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where().and(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.gt(timeuuidCol, UUIDs.endOf(fromDate.getTime())));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
}
