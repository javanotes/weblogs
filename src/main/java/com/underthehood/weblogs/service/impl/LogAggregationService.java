/* ============================================================================
*
* FILE: LogAnalyticsService.java
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
package com.underthehood.weblogs.service.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.underthehood.weblogs.dao.DataAccessException;
import com.underthehood.weblogs.dao.LogEventFinderPagingDAO;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.dto.QueryRequest;
import com.underthehood.weblogs.dto.SliceQueryRequest;
import com.underthehood.weblogs.service.ILogAggregationService;
import com.underthehood.weblogs.service.ServiceException;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;
@Service@Slf4j
public class LogAggregationService implements ILogAggregationService {

  @Autowired@Qualifier("mainDAO")
  private LogEventFinderPagingDAO finder;
  private class QueryRunnable implements Runnable
  {
    private final QueryRequest request;

    public QueryRunnable(QueryRequest request) {
      super();
      this.request = request;
    }

    private volatile boolean isEmpty = true;
    private final List<LogEvent> data = new ArrayList<>();
    private LogEvent fetchMark = null;
    
    private void fetch()
    {
      List<LogEvent> next = finder.findByAppIdBetweenDatesContains(request.getAppId(),
          request.getSearchTerm(), request.getLevel(), fetchMark, request.getFromDate(), request.getTillDate(),
          request.getFetchSize(), false);
      
      isEmpty = (next == null || next.isEmpty());
      
      log.debug("Got next: "+(!isEmpty));
      if(!isEmpty){
        fetchMark = next.get(next.size() -1);
        data.addAll(next);
      }
            
    }
    
    @Override
    public void run() {
      do 
      {
        fetch();
                
      } while (!isEmpty);
    }
      
    
  }
  
  @Override
  public Map<String, Map<Date, Long>> countExecutionTimings(SliceQueryRequest request) throws ServiceException
  {
    if(StringUtils.isEmpty(request.getAppId()))
      throw new ServiceException("[appId] missing");
    
    if(request.getTillDate() == null)
      throw new ServiceException("end date missing");
    if(request.getFromDate() == null)
      throw new ServiceException("start date missing");
    if(!StringUtils.hasText(request.getSearchTerm()))
      throw new ServiceException("execution log start search term missing");
    if(!StringUtils.hasText(request.getSearchTermEnd()))
      throw new ServiceException("execution log end search term missing");
    
    QueryRequest start = new QueryRequest();
    BeanUtils.copyProperties(request, start);
    QueryRequest end = new QueryRequest();
    BeanUtils.copyProperties(request, end);
    end.setSearchTerm(request.getSearchTermEnd());
    
    log.info("Execution start tag >> "+start.getSearchTerm());
    log.info("Execution end tag >> "+end.getSearchTerm());
    
    final QueryRunnable startRunnable = new QueryRunnable(start); 
    final QueryRunnable endRunnable = new QueryRunnable(end);
    
    ExecutorService t = Executors.newFixedThreadPool(2);
    t.submit(startRunnable);
    t.submit(endRunnable);
    t.shutdown();
    try {
      if(!t.awaitTermination(60, TimeUnit.SECONDS))
        throw new ServiceException(new DataAccessException("Query did not complete in configured duration! Resultset could have been inconsistent"));
    } catch (InterruptedException e) {
      log.debug("", e);
      Thread.currentThread().interrupt();
    }
    if(startRunnable.data.isEmpty())
    {
      throw new ServiceException("Execution start tag ["+start.getSearchTerm()+"] not found");
    }
    if(endRunnable.data.isEmpty())
    {
      throw new ServiceException("Execution end tag ["+end.getSearchTerm()+"] not found");
    }
    final Map<String, Map<Date, Long>> mapped = new HashMap<>();
    for(final LogEvent l : startRunnable.data)
    {
      if (StringUtils.hasLength(l.getExecId())) {
        boolean found = false;
        for(Iterator<LogEvent> e  = endRunnable.data.listIterator();e.hasNext();)
        {
          LogEvent ll = e.next();
          if(l.getExecId().equals(ll.getExecId()))
          {
            if(!mapped.containsKey(l.getExecId())){
              mapped.put(l.getExecId(), new TreeMap<Date, Long>());
            }
            mapped.get(l.getExecId()).put(l.getId().getTimestampAsDate(), ll.getId().getTimestampAsLong() - l.getId().getTimestampAsLong());
            e.remove();
            found = true;
            break;
          }
        }
        if(!found)
          log.warn("Execution start tag underflow for start time ["+l.getId().getTimestampAsDate()+"]. "
              + "Probably the execution end is after the given duration or end tag not found");
      }
    }
    if(!endRunnable.data.isEmpty())
      log.debug("Execution end tag/s underflow. Probably the execution start was before the given duration");
    
    return mapped;
  }

  @Override
  public Map<String, Long> countHourlyLogsByLevel(QueryRequest request) throws ServiceException
  {
    Map<Date, Long> counts = countLogsByLevel(request);
    
    final SimpleDateFormat format = new SimpleDateFormat(CommonHelper.LOG_TREND_HOURLY_FORMAT);
    final Map<String, Long> grouped = new TreeMap<>(new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        try {
          return format.parse(o1).compareTo(format.parse(o2));
        } catch (ParseException e) {
          log.error("", e);
          throw new RuntimeException(e);
        }
      }
    });
    String date;
    
    Set<Date> fillHrs = CommonHelper.fillBetweenHours(request.getFromDate(), request.getTillDate());
        
    for(Entry<Date, Long> e : counts.entrySet())
    {
      date = format.format(e.getKey());
      if(!grouped.containsKey(date))
      {
        grouped.put(date, e.getValue());
      }
      else
      {
        grouped.put(date, grouped.get(date)+e.getValue());
      }
    }
    for(Date fill : fillHrs)
    {
      date = format.format(fill);
      if(!grouped.containsKey(date))
      {
        grouped.put(date, 0l);
      }
    }
    return grouped;
    
  }
  
  @Override
  public Map<String, Long> countDailyLogsByLevel(QueryRequest request) throws ServiceException
  {
    Map<Date, Long> counts = countLogsByLevel(request);
    final SimpleDateFormat format = new SimpleDateFormat(CommonHelper.LOG_TREND_DAILY_FORMAT);
    final Map<String, Long> grouped = new TreeMap<>(new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        try {
          return format.parse(o1).compareTo(format.parse(o2));
        } catch (ParseException e) {
          log.error("", e);
          throw new RuntimeException(e);
        }
      }
    });
    
    String date;
    
    Set<Date> fillDays = CommonHelper.fillBetweenDays(request.getFromDate(), request.getTillDate());
    
    for(Entry<Date, Long> e : counts.entrySet())
    {
      date = format.format(e.getKey());
      if(!grouped.containsKey(date))
      {
        grouped.put(date, e.getValue());
      }
      else
      {
        grouped.put(date, grouped.get(date)+e.getValue());
      }
    }
    for(Date fill : fillDays)
    {
      date = format.format(fill);
      if(!grouped.containsKey(date))
      {
        grouped.put(date, 0l);
      }
    }
    return grouped;
    
  }
  private Map<Date, Long> countLogsByLevel(QueryRequest request) throws ServiceException
  {
    if(StringUtils.isEmpty(request.getAppId()))
      throw new ServiceException("[appId] missing");
    if(StringUtils.isEmpty(request.getLevel()))
      throw new ServiceException("[level] missing");
    if(request.getTillDate() == null)
      throw new ServiceException("end date missing");
    if(request.getFromDate() == null)
      throw new ServiceException("start date missing");
    
    if(StringUtils.hasText(request.getSearchTerm()))
    {
      request.setSearchTerm(request.getSearchTerm().toLowerCase());
    }
    
    LogEvent fetchMark = null;
    List<LogEvent> data = null;
    final Map<Date, Long> count = new TreeMap<>();
    do 
    {
      if(data != null && !data.isEmpty()){
        fetchMark = data.get(data.size() -1);
        aggregateCount(count, data);
      }
      
      if(!StringUtils.isEmpty(request.getSearchTerm()))
      {
        data = finder.findByAppIdBetweenDatesContains(request.getAppId(),
            request.getSearchTerm(), request.getLevel(), fetchMark, request.getFromDate(), request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
                
      }
      else
      {
        data = finder.findByAppIdBetweenDates(request.getAppId(), request.getLevel(),
            fetchMark, request.getFromDate(), request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
        
      }
      
    } while (data != null && !data.isEmpty());
    
    return count;
  }

  private static void aggregateCount(Map<Date, Long> count, List<LogEvent> data) {
    Date ts;
    for(LogEvent e : data)
    {
      ts = e.getId().getTimestampAsDate();
      if(!count.containsKey(ts))
      {
        count.put(ts, 1L);
      }
      else
      {
        count.put(ts, count.get(ts)+1);
      }
    }
    
  }


}
