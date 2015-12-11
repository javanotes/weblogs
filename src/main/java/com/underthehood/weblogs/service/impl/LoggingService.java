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
package com.underthehood.weblogs.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.underthehood.weblogs.dao.DataAccessException;
import com.underthehood.weblogs.dao.LogEventFinderPagingDAO;
import com.underthehood.weblogs.dao.LogEventIngestionDAO;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.dto.LogEventDTO;
import com.underthehood.weblogs.dto.LogRequest;
import com.underthehood.weblogs.dto.QueryRequest;
import com.underthehood.weblogs.dto.QueryResponse;
import com.underthehood.weblogs.service.ILoggingService;
import com.underthehood.weblogs.service.ServiceException;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;
@Service@Slf4j
public class LoggingService implements ILoggingService {

  @Autowired
  private LogEventIngestionDAO ingestionDao;
  @Autowired
  private LogEventFinderPagingDAO finder;
  
  @Override
  public void ingestLoggingRequest(LogRequest req) throws ServiceException
  {
    try 
    {
      if(req.getTimestamp() <= 0)
        throw new ServiceException("Timestamp not provided");
      LogEvent l;
      l = new LogEvent(req);
      l.getId().setTimestamp(CommonHelper.makeTimeBasedUUID(req.getTimestamp()));           
      ingestionDao.insert(l);
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
      //We want to maintain the order of the log. However, not concerned on atomicity
      //so sending an unlogged batch. However, batch inserts fail with too large batchsize.
      //Async insert is not guaranteeing the order of inserts. So inserting synchronously.
      
      LogEvent l;
      List<LogEvent> events = new ArrayList<>();
      
      log.debug(">>> ingestLoggingRequests: Starting ingestion batch <<< ");
      long start = System.currentTimeMillis();
      for (LogRequest req : requests) 
      {
        if(req.getTimestamp() <= 0)
          throw new ServiceException("Timestamp not provided");
        l = new LogEvent(req);
        //we are setting the uuid from a jvm process
        //TODO: synchronize to be globally unique uuid?
        l.getId().setTimestamp(CommonHelper.makeTimeBasedUUID(req.getTimestamp()));
        events.add(l);
      }
      
      ingestionDao.ingestEntitiesAsync(events);
      
      long time = System.currentTimeMillis() - start;
      log.info(">>> ingestLoggingRequests: End ingestion batch <<< ("+requests.size()+")");
      long secs = TimeUnit.MILLISECONDS.toSeconds(time);
      log.info("Time taken: "+secs+" secs "+(time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
    } 
    catch (DataAccessException e) {
      log.error("Logging request ingestion failed", e);
      throw new ServiceException(e);
    }
    //handle other exceptions at controller
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
