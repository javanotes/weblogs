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
import java.util.Collections;
import java.util.Comparator;
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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.domain.annot.FullTextSearchable;
import com.ericsson.weblogs.domain.annot.LuceneIndex;
import com.ericsson.weblogs.domain.annot.SearchIndexed;
import com.ericsson.weblogs.utils.CommonHelper;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.search.condition.BooleanCondition;
import com.stratio.cassandra.lucene.builder.search.sort.SortField;

import lombok.extern.slf4j.Slf4j;

/**
 * <b>NOTE:</b> Only {@link #findByAppIdBeforeDateContains()} and {@link #findByAppIdBetweenDates()}
 * have been properly implemented. The rest are TODO
 */
@Slf4j@Repository
public class LogEventFinderPagingDAO extends LogEventDAO{

  private String[] pkCols;
  private String timeuuidCol;
  
  private String luceneCol;
  private String logTextCol;
  private final Map<Integer, String> searchFields = new HashMap<>();
  
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
  /*
   * TODO: Not sure why needing to use long in maxtimeuuid and String in mintimeuuid ???!!!
   */
   
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
  public List<LogEvent> findByAppIdBetweenDatesContains(final String appId, String token, String level, LogEvent lastRow, final Date fromDate, final Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
      
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
        w.and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)))
        .and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
        .and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
     
    }  
    else
    {
      w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)));
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
        w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
      }
    }  
    else
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
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
        w.and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
        w.and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        
      }
    }  
    else
    {
      w.and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
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
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if (StringUtils.hasText(level)) {
      if(searchFields.containsKey(0)){
        w.and(QueryBuilder.eq(luceneCol,
            Builder.search().filter(Builder.match(searchFields.get(0), level))
                .sort(SortField.field(timeuuidCol).reverse(isPrevPaging))
                .build()));
      }
      else
        log.warn("-- LOG LEVEL SEARCH WILL BE IGNORED, since the @SearchIndexed annotation was not configured!");
      
    }
    if(lastRow != null)
    {
      if(isPrevPaging){
        w.and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)))
        .and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
        .and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
     
    }  
    else
    {
      w.and(QueryBuilder.gte(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(fromDate))))
      .and(QueryBuilder.lte(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, toDate)));
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
  
  public List<LogEvent> findByAppIdBeforeDate(String appId, LogEvent lastRow, Date toDate, int limit, boolean isPrevPaging)
  {
    Select sel = QueryBuilder.select().from(table).limit(limit);
    Where w = sel.where(QueryBuilder.eq(pkCols[0], appId));
    if(lastRow != null)
    {
      if(isPrevPaging){
        w.and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
        w.and(QueryBuilder.gt(timeuuidCol, lastRow.getId().getTimestamp()));
      }
      else{
        w.and(QueryBuilder.lt(timeuuidCol, lastRow.getId().getTimestamp()));
        
      }
    }  
    else
    {
      w.and(QueryBuilder.lt(timeuuidCol, QueryBuilder.fcall(MINTIMEUUID, CommonHelper.formatAsCassandraDate(toDate))));
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
        w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
      }
    }  
    else
    {
      w.and(QueryBuilder.gt(timeuuidCol, QueryBuilder.fcall(MAXTIMEUUID, fromDate)));
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
    Row row = cassandraOperations.query(sel).one();
    return row.getLong(0);
    
  }
}
