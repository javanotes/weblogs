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
import com.ericsson.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;

/**
 * @deprecated - Range queries use ALLOW FILTERING. Not updated with domain. Use {@linkplain LogEventFinderPagingDAO}
 */
@Slf4j@Repository
class LogEventFinderDAO extends LogEventDAO{

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
  
  
  public List<LogEvent> findByAppId(final String appId)
 {
    Select sel = QueryBuilder.select().from(table);
    sel.where(QueryBuilder.eq(pkCols[0], appId)).and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpochDate().getTime())));

    log.debug(">>>>>>>>> Firing select query: " + sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
   
  public List<LogEvent> findByAppIdContains(final String appId, String token)
  {
    Select sel = QueryBuilder.select().from(table);
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.contains(tokenCol, token));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
    
    
  }
  
    
  public List<LogEvent> findByAppIdBetweenDatesContains(final String appId, String token, final Date fromDate, final Date toDate)
  {
    Select sel = QueryBuilder.select().from(table).allowFiltering();
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.gt(pkCols[2], fromDate))
    .and(QueryBuilder.lt(pkCols[2], toDate))
    .and(QueryBuilder.contains(tokenCol, token));
        
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  
  public List<LogEvent> findByAppIdAfterDateContains(final String appId, String token, final Date fromDate)
  {
    Select sel = QueryBuilder.select().from(table).allowFiltering();
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.gt(pkCols[2], fromDate))
    .and(QueryBuilder.contains(tokenCol, token));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
    
  public List<LogEvent> findByAppIdBeforeDateContains(String appId, String token, final Date toDate)
  {
    Select sel = QueryBuilder.select().from(table).allowFiltering();
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.lt(pkCols[2], toDate))
    .and(QueryBuilder.contains(tokenCol, token));
        
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBetweenDates(String appId, Date fromDate, Date toDate)
  {
    Select sel = QueryBuilder.select().from(table).allowFiltering();
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.gt(pkCols[2], fromDate))
    .and(QueryBuilder.lt(pkCols[2], toDate));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBeforeDate(String appId, Date toDate)
  {
    Select sel = QueryBuilder.select().from(table).allowFiltering();
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.lt(pkCols[2], toDate));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdAfterDate(String appId, Date fromDate)
  {
    Select sel = QueryBuilder.select().from(table).allowFiltering();
    sel.where(QueryBuilder.eq(pkCols[0], appId))/*.and(QueryBuilder
        .gte(timeuuidCol, UUIDs.startOf(CommonHelper.javaEpoch().getTime())))*/
    .and(QueryBuilder.gt(pkCols[2], fromDate));
    
    log.debug(">>>>>>>>> Firing select query: "+sel.getQueryString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
}
