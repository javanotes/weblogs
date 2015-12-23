/* ============================================================================
*
* FILE: LogEventFinderDAO.java
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
package com.underthehood.weblogs.dao;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.springframework.stereotype.Repository;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;

/**
 * @deprecated - No proper Cassandra data model for non date range bucketing
 */
@Slf4j@Repository
class LogEventFinderPagingDAOExtn extends LogEventFinderPagingDAO
{
  
  public List<LogEvent> findByAppId(final String appId, LogEvent lastRow, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if (lastRow != null) {
      w.and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    } 
    
    if(isPrevPaging)
      sel.orderBy(QueryBuilder.asc(timeuuidCol));
            
    log.debug(">>>>>>>>> Firing select query: " + sel.toString());
    List<LogEvent> events =  cassandraOperations.select(sel, LogEvent.class);
    if(isPrevPaging){
      Collections.sort(events, new Comparator<LogEvent>() {

        @Override
        public int compare(LogEvent o1, LogEvent o2) {
          return o2.getId().getTimestamp().compareTo(o1.getId().getTimestamp());
        }
      });
    }
    return events;
  }
   
  public List<LogEvent> findByAppIdContains(final String appId, String token, LogEvent lastRow, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
   
    w.and(QueryBuilder.eq(luceneCol, Builder.search()
        .filter(Builder.phrase(logTextCol, token))
        .sort(SortField.field(timeuuidCol).reverse(isPrevPaging))
        .build()));
    
    if(lastRow != null)
    {
      w.and(isPrevPaging
          ? QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp())
          : QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
    }
    
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    List<LogEvent> events =  cassandraOperations.select(sel, LogEvent.class);
    if(isPrevPaging){
      Collections.sort(events, new Comparator<LogEvent>() {

        @Override
        public int compare(LogEvent o1, LogEvent o2) {
          return o2.getId().getTimestamp().compareTo(o1.getId().getTimestamp());
        }
      });
    }
    return events;
    
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
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    
    w.and(QueryBuilder.eq(luceneCol, Builder.search()
        .filter(Builder.phrase(logTextCol, token))
        .sort(SortField.field(timeuuidCol).reverse(isPrevPaging))
        .build()));
    if(lastRow != null)
    {
      if(isPrevPaging){
        w.and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(FN_MAXTIMEUUID, fromDate)));
      }
    }  
    else
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(FN_MAXTIMEUUID, fromDate)));
    }
    
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    
    List<LogEvent> events =  cassandraOperations.select(sel, LogEvent.class);
    if(isPrevPaging){
      Collections.sort(events, new Comparator<LogEvent>() {

        @Override
        public int compare(LogEvent o1, LogEvent o2) {
          return o2.getId().getTimestamp().compareTo(o1.getId().getTimestamp());
        }
      });
    }
    return events;
  }
    
  public List<LogEvent> findByAppIdBeforeDateContains(String appId, String token, LogEvent lastRow, final Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    
    w.and(QueryBuilder.eq(luceneCol, Builder.search()
        .filter(Builder.phrase(logTextCol, token))
        .sort(SortField.field(timeuuidCol).reverse(isPrevPaging))
        .build()));
    if(lastRow != null)
    {
      if(isPrevPaging){
        w.and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(FN_MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
        w.and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        
      }
    }  
    else
    {
      w.and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(FN_MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
    } 
    
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    
    List<LogEvent> events =  cassandraOperations.select(sel, LogEvent.class);
    if(isPrevPaging){
      Collections.sort(events, new Comparator<LogEvent>() {

        @Override
        public int compare(LogEvent o1, LogEvent o2) {
          return o2.getId().getTimestamp().compareTo(o1.getId().getTimestamp());
        }
      });
    }
    return events;
  }
   
  public List<LogEvent> findByAppIdAfterDate(String appId, LogEvent lastRow, Date fromDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if(lastRow != null)
    {
      if(isPrevPaging){
        w.and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(FN_MAXTIMEUUID, fromDate)));
      }
    }  
    else
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(FN_MAXTIMEUUID, fromDate)));
    }
    if(isPrevPaging)
      sel.orderBy(QueryBuilder.asc(timeuuidCol));
    log.debug(">>>>>>>>> Firing select query: "+sel.toString());
    List<LogEvent> events =  cassandraOperations.select(sel, LogEvent.class);
    if(isPrevPaging){
      Collections.sort(events, new Comparator<LogEvent>() {

        @Override
        public int compare(LogEvent o1, LogEvent o2) {
          return o2.getId().getTimestamp().compareTo(o1.getId().getTimestamp());
        }
      });
    }
    return events;
  }
  
}