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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ericsson.weblogs.dao.DataAccessException;
import com.ericsson.weblogs.dao.LogEventIngestionDAO;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.dto.LogRequest;
import com.ericsson.weblogs.lucene.FullTextSearch;
import com.ericsson.weblogs.service.ILoggingService;
import com.ericsson.weblogs.service.ServiceException;

import lombok.extern.slf4j.Slf4j;
@Service@Slf4j
public class LoggingService implements ILoggingService {

  @Autowired
  private LogEventIngestionDAO ingestor;
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
}
