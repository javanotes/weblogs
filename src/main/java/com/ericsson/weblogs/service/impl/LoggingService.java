/* ============================================================================
*
* FILE: LoggingService.java
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
package com.ericsson.weblogs.service.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.ericsson.weblogs.dao.DataAccessException;
import com.ericsson.weblogs.dao.LogEventFinderPagingDAO;
import com.ericsson.weblogs.dao.LogEventIngestionDAO;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.dto.LogEventDTO;
import com.ericsson.weblogs.dto.LogRequest;
import com.ericsson.weblogs.dto.QueryRequest;
import com.ericsson.weblogs.dto.QueryResponse;
import com.ericsson.weblogs.service.ILoggingService;
import com.ericsson.weblogs.service.ServiceException;
import com.ericsson.weblogs.utils.CommonHelper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import lombok.extern.slf4j.Slf4j;
@Service@Slf4j
public class LoggingService implements ILoggingService {

  @Autowired
  private LogEventIngestionDAO ingestor;
  @Autowired
  private LogEventFinderPagingDAO finder;
  
  @Override
  public void ingestLoggingRequest(LogRequest req) throws ServiceException
  {
    try 
    {
      
      LogEvent l;
      l = new LogEvent(req);
                 
      ingestor.insert(l);
    } 
    catch (DataAccessException e) {
      log.error("Logging request ingestion failed", e);
      throw new ServiceException(e);
    }
    
  }
  
  @Override
  public void ingestLoggingRequests(List<LogRequest> requests) throws ServiceException
  {
    try 
    {
      
      LogEvent l;
      List<LogEvent> events = new ArrayList<>();
      
      for (LogRequest req : requests) 
      {
        l = new LogEvent(req);
              
        events.add(l);
      }

      ingestor.ingestAsync(events);
    } 
    catch (DataAccessException e) {
      log.error("Logging request ingestion failed", e);
      throw new ServiceException(e);
    }
    //handle other exceptions at controller
  }

  @Override
  public QueryResponse fetchLogsFromDate(QueryRequest request)
      throws ServiceException {
    if(StringUtils.isEmpty(request.getAppId()))
      throw new ServiceException("[appId] missing");
    if(request.getFromDate() == null)
      throw new ServiceException("start date missing");
    
    
    if(StringUtils.hasText(request.getSearchTerm()))
    {
      request.setSearchTerm(request.getSearchTerm().toLowerCase());
    }
    final List<LogEventDTO> events = new ArrayList<>();
    List<LogEvent> data;long count;
    LogEvent fetchMark = StringUtils.hasText(request.getFetchMarkUUID())
        ? new LogEvent(UUID.fromString(request.getFetchMarkUUID())) : null;
    try 
    {
      if (!StringUtils.isEmpty(request.getSearchTerm())) 
      {
        data = finder.findByAppIdAfterDateContains(request.getAppId(),
            request.getSearchTerm(), fetchMark, request.getFromDate(),
            request.getFetchSize(), request.isFetchPrev());
        
        count = finder.count(request.getAppId(), request.getSearchTerm(), null, request.getFromDate(), null);
      } 
      else 
      {
        data = finder.findByAppIdAfterDate(request.getAppId(), fetchMark,
            request.getFromDate(), request.getFetchSize(),
            request.isFetchPrev());
        
        count = finder.count(request.getAppId(), null, null, request.getFromDate(), null);
      }
    } 
    catch (org.springframework.dao.DataAccessException e) {
      log.error("While querying data ", e);
      throw new ServiceException(e);
    }
    
    final QueryResponse resp = new QueryResponse();
    resp.setITotalRecords(count);
    if(data != null && !data.isEmpty())
    {
      resp.setFirstRowUUID(data.get(0).getId().getTimestamp().toString());
      resp.setLastRowUUID(data.get(data.size()-1).getId().getTimestamp().toString());
      
      events.addAll(
      Collections2.transform(data, new Function<LogEvent, LogEventDTO>() {

        @Override
        public LogEventDTO apply(LogEvent input) {
          return new LogEventDTO(input);
        }
      }));
      
      resp.getLogs().addAll(events);
      resp.setITotalDisplayRecords(count);
      
      
    }
    return resp;
  }

  @Override
  public QueryResponse fetchLogsTillDate(QueryRequest request)
      throws ServiceException {
    if(StringUtils.isEmpty(request.getAppId()))
      throw new ServiceException("[appId] missing");
    if(request.getTillDate() == null)
      throw new ServiceException("end date missing");
    
    if(StringUtils.hasText(request.getSearchTerm()))
    {
      request.setSearchTerm(request.getSearchTerm().toLowerCase());
    }
    final List<LogEventDTO> events = new ArrayList<>();
    List<LogEvent> data;long count;
    LogEvent fetchMark = StringUtils.hasText(request.getFetchMarkUUID())
        ? new LogEvent(UUID.fromString(request.getFetchMarkUUID())) : null;
        
    try 
    {
      if(!StringUtils.isEmpty(request.getSearchTerm()))
      {
        data = finder.findByAppIdBeforeDateContains(request.getAppId(),
            request.getSearchTerm(), fetchMark, request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
        
        count = finder.count(request.getAppId(), "", request.getSearchTerm(), null, request.getTillDate());
      }
      else
      {
        data = finder.findByAppIdBeforeDate(request.getAppId(),
            fetchMark, request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
        
        count = finder.count(request.getAppId(), "", null, null, request.getTillDate());
      }
    } catch (org.springframework.dao.DataAccessException e) {
      log.error("While querying data ", e);
      throw new ServiceException(e);
    }
    
    final QueryResponse resp = new QueryResponse();
    resp.setITotalRecords(count);
    if(data != null && !data.isEmpty())
    {
      resp.setFirstRowUUID(data.get(0).getId().getTimestamp().toString());
      resp.setLastRowUUID(data.get(data.size()-1).getId().getTimestamp().toString());
      
      events.addAll(
      Collections2.transform(data, new Function<LogEvent, LogEventDTO>() {

        @Override
        public LogEventDTO apply(LogEvent input) {
          return new LogEventDTO(input);
        }
      }));
      
      resp.getLogs().addAll(events);
      resp.setITotalDisplayRecords(count);
            
    }
    return resp;
  }
  public Map<String, Long> countHourlyLogsByLevel(QueryRequest request) throws ServiceException
  {
    Map<Date, Long> counts = countLogsByLevel(request);
    final Map<String, Long> grouped = new LinkedHashMap<>();
    SimpleDateFormat format = new SimpleDateFormat(CommonHelper.LOG_TREND_HOURLY_FORMAT);
    String date;
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
    return grouped;
    
  }
  public Map<String, Long> countDailyLogsByLevel(QueryRequest request) throws ServiceException
  {
    Map<Date, Long> counts = countLogsByLevel(request);
    final Map<String, Long> grouped = new LinkedHashMap<>();
    SimpleDateFormat format = new SimpleDateFormat(CommonHelper.LOG_TREND_DAILY_FORMAT);
    String date;
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
    return grouped;
    
  }
  public Map<Date, Long> countLogsByLevel(QueryRequest request) throws ServiceException
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

  private void aggregateCount(Map<Date, Long> count, List<LogEvent> data) {
    for(LogEvent e : data)
    {
      if(!count.containsKey(e.getId().getTimestampAsDate()))
      {
        count.put(e.getId().getTimestampAsDate(), 1L);
      }
      else
      {
        count.put(e.getId().getTimestampAsDate(), count.get(e.getId().getTimestampAsDate())+1);
      }
    }
    
  }

  @Override
  public QueryResponse fetchLogsBetweenDates(QueryRequest request)
      throws ServiceException {
    if(StringUtils.isEmpty(request.getAppId()))
      throw new ServiceException("[appId] missing");
    if(request.getTillDate() == null)
      throw new ServiceException("end date missing");
    if(request.getFromDate() == null)
      throw new ServiceException("start date missing");
    
    if(StringUtils.hasText(request.getSearchTerm()))
    {
      request.setSearchTerm(request.getSearchTerm().toLowerCase());
    }
    final List<LogEventDTO> events = new ArrayList<>();
    List<LogEvent> data;
    LogEvent fetchMark = StringUtils.hasText(request.getFetchMarkUUID())
        ? new LogEvent(UUID.fromString(request.getFetchMarkUUID())) : null;
       
    long count;
    try 
    {
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
      count = finder.count(request.getAppId(), request.getSearchTerm(), request.getLevel(), request.getFromDate(), request.getTillDate());
      
    } catch (org.springframework.dao.DataAccessException e) {
      log.error("While querying data ", e);
      throw new ServiceException(e);
    }
    final QueryResponse resp = new QueryResponse();
    resp.setITotalRecords(count);
    if(data != null && !data.isEmpty())
    {
      resp.setFirstRowUUID(data.get(0).getId().getTimestamp().toString());
      resp.setLastRowUUID(data.get(data.size()-1).getId().getTimestamp().toString());
      
      events.addAll(
      Collections2.transform(data, new Function<LogEvent, LogEventDTO>() {

        @Override
        public LogEventDTO apply(LogEvent input) {
          return new LogEventDTO(input);
        }
      }));
      
      resp.getLogs().addAll(events);
      resp.setITotalDisplayRecords(count);
      
      
    }
    return resp;
  }
}
