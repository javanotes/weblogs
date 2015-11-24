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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
import com.ericsson.weblogs.lucene.FullTextSearch;
import com.ericsson.weblogs.service.ILoggingService;
import com.ericsson.weblogs.service.ServiceException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import lombok.extern.slf4j.Slf4j;
@Service@Slf4j
public class LoggingService implements ILoggingService {

  @Autowired
  private LogEventIngestionDAO ingestor;
  @Autowired
  private LogEventFinderPagingDAO finder;
  @Autowired
  private FullTextSearch ftsEngine;
  
  @Override
  public void ingestLoggingRequest(LogRequest req) throws ServiceException
  {
    try 
    {
      
      LogEvent l;
      Set<String> t;
      
      l = new LogEvent(req);
      t = ftsEngine.tokenizeText(req.getLogText(), req.getSearchTerms(), false);
      
      if (log.isDebugEnabled()) {
        log.debug(
            "Got tokens: " + t + " for log text => " + req.getLogText());
      }
      l.getTokens().addAll(t);
      
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
      Set<String> t;
      List<LogEvent> events = new ArrayList<>();
      
      for (LogRequest req : requests) 
      {
        l = new LogEvent(req);
        t = ftsEngine.tokenizeText(req.getLogText(), req.getSearchTerms(), false);
        if (log.isDebugEnabled()) {
          log.debug(
              "Got tokens: " + t + " for log text => " + req.getLogText());
        }
        l.getTokens().addAll(t);
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
        
        count = finder.count(request.getAppId(), request.getSearchTerm(), request.getFromDate(), null);
      } 
      else 
      {
        data = finder.findByAppIdAfterDate(request.getAppId(), fetchMark,
            request.getFromDate(), request.getFetchSize(),
            request.isFetchPrev());
        
        count = finder.count(request.getAppId(), null, request.getFromDate(), null);
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
        
        count = finder.count(request.getAppId(), request.getSearchTerm(), null, request.getTillDate());
      }
      else
      {
        data = finder.findByAppIdBeforeDate(request.getAppId(),
            fetchMark, request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
        
        count = finder.count(request.getAppId(), null, null, request.getTillDate());
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
            request.getSearchTerm(), fetchMark, request.getFromDate(), request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
        
        count = finder.count(request.getAppId(), request.getSearchTerm(), request.getFromDate(), request.getTillDate());
      }
      else
      {
        data = finder.findByAppIdBetweenDates(request.getAppId(),
            fetchMark, request.getFromDate(), request.getTillDate(),
            request.getFetchSize(), request.isFetchPrev());
        
        count = finder.count(request.getAppId(), null, request.getFromDate(), request.getTillDate());
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
}
