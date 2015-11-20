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
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;


@Slf4j@Repository
public class LogEventFinderPagingDAO extends LogEventDAO{

  private String[] pkCols;
  private String timeuuidCol;
  private String tokenCol;
  
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
  
  
  public List<LogEvent> findByAppId(final String appId, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum));

    log.debug(">>>>>>>>> Firing select query: " + sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
   
  public List<LogEvent> findByAppIdContains(final String appId, String token, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.contains(tokenCol, token));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
    
    
  }
  
    
  public List<LogEvent> findByAppIdBetweenDatesContains(final String appId, String token, final Date fromDate, final Date toDate, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.gt(pkCols[2], fromDate))
    .and(QueryBuilder.lt(pkCols[2], toDate))
    .and(QueryBuilder.contains(tokenCol, token));
        
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  
  public List<LogEvent> findByAppIdAfterDateContains(final String appId, String token, final Date fromDate, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.gt(pkCols[2], fromDate))
    .and(QueryBuilder.contains(tokenCol, token));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
    
  public List<LogEvent> findByAppIdBeforeDateContains(String appId, String token, final Date toDate, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.lt(pkCols[2], toDate))
    .and(QueryBuilder.contains(tokenCol, token));
        
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBetweenDates(String appId, Date fromDate, Date toDate, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.gt(pkCols[2], fromDate))
    .and(QueryBuilder.lt(pkCols[2], toDate));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBeforeDate(String appId, Date toDate, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.lt(pkCols[2], toDate));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdAfterDate(String appId, Date fromDate, UUID lastRownum, int limit)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gt(timeuuidCol, lastRownum == null ? UUIDs.endOf(CommonHelper.javaEpochDate().getTime()) : lastRownum))
    .and(QueryBuilder.gt(pkCols[2], fromDate));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public long count(final String appId, String token, final Date fromDate, final Date toDate)
  {
    Select sel = QueryBuilder.select().countAll().from(table);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if(fromDate != null || toDate != null){
      w.and(QueryBuilder
          .gt(timeuuidCol, UUIDs.endOf(CommonHelper.javaEpochDate().getTime())));
    }
    if(fromDate != null){
      w.and(QueryBuilder.gt(pkCols[2], fromDate));
    }
    if(toDate != null){
      w.and(QueryBuilder.lt(pkCols[2], toDate));
    }
      
    if(StringUtils.hasText(token)){
      w.and(QueryBuilder.contains(tokenCol, token));
    }
      
    
    log.debug(">>>>>>>>> Firing count(*) query: "+sel.getQueryString());
    Row row = cassandraOperations.query(sel).one();
    return row.getLong(0);
    
  }
}
