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

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.Search;
import com.stratio.cassandra.lucene.builder.search.condition.BooleanCondition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.domain.annot.FullTextSearchable;
import com.underthehood.weblogs.domain.annot.LuceneIndex;
import com.underthehood.weblogs.domain.annot.SearchIndexed;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;


@Slf4j@Repository("mainDAO")
public class LogEventFinderPagingDAO extends LogEventDAO{

  protected String[] pkCols;
  protected String timeuuidCol;
  
  protected String luceneCol;
  protected String logTextCol;
  protected final Map<Integer, String> searchFields = new HashMap<>();
  
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
      
      if(f.isAnnotationPresent(LuceneIndex.class))
      {
        luceneCol = f.getAnnotation(Column.class).value();
        
      }
      if(f.isAnnotationPresent(FullTextSearchable.class))
      {
        logTextCol = f.getAnnotation(Column.class).value();
      }
      if(f.isAnnotationPresent(SearchIndexed.class))
      {
        searchFields.put(f.getAnnotation(SearchIndexed.class).ordinal(), f.getAnnotation(Column.class).value());
      }
    }
    
    s.deleteCharAt(s.lastIndexOf(","));
    s.append(" from ").append(table).append(" where ");
    
    log.debug(">>>>> Prepared dynamic projection: "+s);
    
    
  }
   
  /**
   * Both dates inclusive
   * @param appId
   * @param token
   * @param level
   * @param lastRow
   * @param fromDate
   * @param toDate
   * @param limit
   * @param isPrevPaging
   * @return
   */
  public List<LogEvent> findByAppIdBetweenDatesContains(final String appId, String token, String level, LogEvent lastRow, final Date fromDate, final Date toDate, int limit, final boolean isPrevPaging)
  {
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.TIMEBUCKET_DATEFORMAT);
    
    QueryAggregator qAggr = new QueryAggregator(3, limit);
    qAggr.start();
    List<Date> dateRange = CommonHelper.getBetweenDateBuckets(fromDate, toDate);
    for(Date dr : dateRange)
    {
      Select sel = QueryBuilder.select().from(table).limit(limit);
      Where w = sel.where(
          QueryBuilder.eq(pkCols[0], appId))
          .and(QueryBuilder.eq(pkCols[1], sdf.format(dr)));
      
      BooleanCondition filter = Builder.bool();
      filter.must(Builder.phrase(logTextCol, token));
      if(StringUtils.hasText(level)){
        if(searchFields.containsKey(0))
          filter.must(Builder.match(searchFields.get(0), level));
        else
          log.warn("-- LOG LEVEL SEARCH WILL BE IGNORED, since the @SearchIndexed annotation was not configured!");
        
      }
      
      w.and(QueryBuilder.eq(luceneCol, Builder.search().filter(filter).sort(SortField.field(timeuuidCol).reverse(isPrevPaging)).build()));
      
      if(lastRow != null)
      {
        if(isPrevPaging){
          w.and(QueryBuilder.lte(timeuuidCol, CommonHelper.maxDateUuid(toDate)))
          .and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
        }
        else{
         
          w.and(QueryBuilder.gte(timeuuidCol, CommonHelper.minDateUuid(fromDate)))
          .and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        }
       
      }  
      else
      {
        
        w.and(QueryBuilder.gte(timeuuidCol, CommonHelper.minDateUuid(fromDate)))
        .and(QueryBuilder.lte(timeuuidCol, CommonHelper.maxDateUuid(toDate)));
      }
      
      log.debug(">>>>>>>>> Firing select query: "+sel.toString());
      qAggr.submit(cassandraOperations.executeAsynchronously(sel));
      
    }
    
    List<LogEvent> events = qAggr.awaitResultUninterruptibly();
    
    return events;
  }
  
  
  /**
   * both dates inclusive
   * @param appId
   * @param level
   * @param lastRow
   * @param fromDate
   * @param toDate
   * @param limit
   * @param isPrevPaging
   * @return
   */
  public List<LogEvent> findByAppIdBetweenDates(String appId, String level, LogEvent lastRow, Date fromDate, Date toDate, int limit, boolean isPrevPaging)
  {
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.TIMEBUCKET_DATEFORMAT);
    QueryAggregator qAggr = new QueryAggregator(3, limit);
    qAggr.start();
    List<Date> dateRange = CommonHelper.getBetweenDateBuckets(fromDate, toDate);
    
    for(Date dr : dateRange)
    {
      Select sel = QueryBuilder.select().from(table).limit(limit);
      Where w = sel.where(
          QueryBuilder.eq(pkCols[0], appId))
          .and(QueryBuilder.eq(pkCols[1], sdf.format(dr)));
      
      Search filter = Builder.search().sort(SortField.field(timeuuidCol).reverse(isPrevPaging));
      if (StringUtils.hasText(level)) {
        if(searchFields.containsKey(0)){
          filter.filter(Builder.match(searchFields.get(0), level));
          
        }
        else
          log.warn("-- LOG LEVEL SEARCH WILL BE IGNORED, since the @SearchIndexed annotation was not configured!");
        
      }
      w.and(QueryBuilder.eq(luceneCol, filter.build()));
      
      if(lastRow != null)
      {
        if(isPrevPaging){
         
          w.and(QueryBuilder.lte(timeuuidCol, CommonHelper.maxDateUuid(toDate)))
          .and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
        }
        else{
          
          w.and(QueryBuilder.gte(timeuuidCol, CommonHelper.minDateUuid(fromDate)))
          .and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        }
       
      }  
      else
      {
        
        w.and(QueryBuilder.gte(timeuuidCol, CommonHelper.minDateUuid(fromDate)))
        .and(QueryBuilder.lte(timeuuidCol, CommonHelper.maxDateUuid(toDate)));
      }
      
      log.debug(">>>>>>>>> Firing select query: "+sel.toString());
      qAggr.submit(cassandraOperations.executeAsynchronously(sel));
    }
    
    List<LogEvent> events = qAggr.awaitResultUninterruptibly();
    
   
    return events;
  }
  
    
  /**
   * 
   * @param appId
   * @param token
   * @param level
   * @param fromDate
   * @param toDate
   * @return
   */
  public long count(final String appId, String token, String level, final Date fromDate, final Date toDate)
  {
    if(fromDate == null || toDate == null){
      throw new UnsupportedOperationException("Count query that does not specify a date range not implemented");
    }
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.TIMEBUCKET_DATEFORMAT);
    
    List<Date> dateRange = CommonHelper.getBetweenDateBuckets(fromDate, toDate);
    
    List<ResultSetFuture> f = new ArrayList<>();
    for(Date dr : dateRange)
    {
      Select sel = QueryBuilder.select().countAll().from(table);
      Where w = sel.where(
          QueryBuilder.eq(pkCols[0], appId))
          .and(QueryBuilder.eq(pkCols[1], sdf.format(dr)));
      
      //Select on indexed columns and with IN clause for the PRIMARY KEY are not supported
      if(fromDate != null && toDate != null){
        w.and(QueryBuilder.gte(timeuuidCol, CommonHelper.minDateUuid(fromDate)))
        .and(QueryBuilder.lte(timeuuidCol, CommonHelper.maxDateUuid(toDate)));
      }
      //should be unreachable
      else {
        throw new UnsupportedOperationException("Count query that does not specify a date range not implemented");
        /*w.and(
            QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(FN_MAXTIMEUUID, fromDate)));*/
      }
              
      BooleanCondition logic = Builder.bool();
      boolean wasFiltered = false;
      if(StringUtils.hasText(level)){
        logic.must(Builder.match(searchFields.get(0), level));
        wasFiltered = true;
      }
      if(StringUtils.hasText(token)){
        logic.must(Builder.phrase(logTextCol, token));
        wasFiltered = true;
      }
      if(wasFiltered){
        w.and(QueryBuilder.eq(luceneCol, Builder.search().filter(logic).build()));
      }
      log.debug(">>>>>>>>> Firing count(*) query: "+sel.toString());
      f.add(cassandraOperations.executeAsynchronously(sel));
      
    }
    long l = 0;
    for(ResultSetFuture r : f)
    {
      l += r.getUninterruptibly().one().getLong(0);
    }
    
    return l;
  }
}
