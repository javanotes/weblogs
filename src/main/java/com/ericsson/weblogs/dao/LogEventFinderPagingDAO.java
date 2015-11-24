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
      if(f.getType() == UUID.class)
      {
        timeuuidCol = pkCols[i-1];
      }
    }
    
    StringBuilder s = new  StringBuilder("select ");
    for(Field f : allFields.values())
    {
      String c = f.isAnnotationPresent(PrimaryKeyColumn.class)
          ? f.getAnnotation(PrimaryKeyColumn.class).name()
          : f.getAnnotation(Column.class).value();
          
      s.append(c).append(",");
      
      if(Set.class.isAssignableFrom(f.getType()))
      {
        tokenCol = f.getAnnotation(Column.class).value();
      }
    }
    
    s.deleteCharAt(s.lastIndexOf(","));
    s.append(" from ").append(table).append(" where ");
    
    log.debug(">>>>> Prepared dynamic projection: "+s);
    
    
  }
  
  
  public List<LogEvent> findByAppId(final String appId, LogEvent lastRow, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if (lastRow != null) {
      w.and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    
    log.debug(">>>>>>>>> Firing select query: " + sel.toString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
   
  public List<LogEvent> findByAppIdContains(final String appId, String token, LogEvent lastRow, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.contains(tokenCol, token));
    if(lastRow != null)
    {
      w.and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    
    return cassandraOperations.select(sel, LogEvent.class);
    
    
  }
  /*
   * TODO: Not sure why needing to use long in maxtimeuuid and String in mintimeuuid ???!!!
   */
    
  public List<LogEvent> findByAppIdBetweenDatesContains(final String appId, String token, LogEvent lastRow, final Date fromDate, final Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId))
   
    .and(QueryBuilder.contains(tokenCol, token));
    if(lastRow != null)
    {
      w.and(isPrevPaging ? QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)) : QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    else
    {
      w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)));
    }
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  /**
   * 
   * @param appId
   * @param token
   * @param lastRow
   * @param fromDate
   * @param limit
   * @param isPrevPaging
   * @return
   */
  public List<LogEvent> findByAppIdAfterDateContains(final String appId, String token, LogEvent lastRow, final Date fromDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.contains(tokenCol, token));
    if(lastRow != null)
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)))
      .and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    else
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
    }
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
    
  public List<LogEvent> findByAppIdBeforeDateContains(String appId, String token, LogEvent lastRow, final Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId))
    .and(QueryBuilder.contains(tokenCol, token));
    if(lastRow != null)
    {
      w
      .and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    else
    {
      w
      .and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
    }    
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    
    return cassandraOperations.select(sel, LogEvent.class);
  }
  /**
   * both dates inclusive
   * @param appId
   * @param lastRow
   * @param fromDate
   * @param toDate
   * @param limit
   * @param isPrevPaging
   * @return
   */
  public List<LogEvent> findByAppIdBetweenDates(String appId, LogEvent lastRow, Date fromDate, Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if(lastRow != null)
    {
      w.and(isPrevPaging ? QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)) : QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    else
    {
      w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)));
    }
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdBeforeDate(String appId, LogEvent lastRow, Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if(lastRow != null)
    {
      w
      .and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    else
    {
      w
      .and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
    }
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public List<LogEvent> findByAppIdAfterDate(String appId, LogEvent lastRow, Date fromDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if(lastRow != null)
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)))
      .and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }  
    else
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
    }
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    return cassandraOperations.select(sel, LogEvent.class);
  }
  
  public long count(final String appId, String token, final Date fromDate, final Date toDate)
  {
    Select sel = QueryBuilder.select().countAll().from(table);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    
    if(fromDate != null && toDate != null){
      w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)));
    }
    else if (fromDate != null) {
      w.and(
          QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
    }
    else if(toDate != null){
      w.and(
          QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
    }
      
    if(StringUtils.hasText(token)){
      w.and(QueryBuilder.contains(tokenCol, token));
    }
      
    
    log.debug(">>>>>>>>> Firing count(*) query: "+sel.toString());
    Row row = cassandraOperations.query(sel).one();
    return row.getLong(0);
    
  }
}
