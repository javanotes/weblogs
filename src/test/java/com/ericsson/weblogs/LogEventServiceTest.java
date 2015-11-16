/* ============================================================================
*
* FILE: LogEventRepositoryTest.java
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
package com.ericsson.weblogs;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.ericsson.weblogs.dao.LogEventRepository;
import com.ericsson.weblogs.dto.LogRequest;
import com.ericsson.weblogs.dto.QueryRequest;
import com.ericsson.weblogs.dto.QueryResponse;
import com.ericsson.weblogs.service.ILoggingService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class LogEventServiceTest {
  
  @Autowired
  private ILoggingService logService;
  
  @Autowired
  private LogEventRepository repo;
  
  LogRequest event;
  @After
  public void delete()
  {
    if(event != null)
    {
      try {
        MapId id = event.toMapId();
        repo.delete(id);
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Test
  public void testInsertLogEvent()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    try {
      logService.ingestLoggingRequest(event);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
  final int batchSize = 10;
  final String appId = "applicationId";
  @Test
  public void testInsertLogEvents()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    List<LogRequest> requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      l = new LogRequest();
      l.setLogText("This is some bla blaah bla logging at info level");
      l.setApplicationId(appId);
      
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
  @Test
  public void testFindLogEvents()
  {
    testInsertLogEvents();
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    
    Calendar yesterday = GregorianCalendar.getInstance();
    yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
    req.setFromDate(yesterday.getTime());
    
    Calendar tomorrow = GregorianCalendar.getInstance();
    tomorrow.set(Calendar.DATE, yesterday.get(Calendar.DATE)+1);
    req.setTillDate(tomorrow.getTime());
    
    QueryResponse resp;
    try 
    {
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      
      req.setAppId("doNotMatch");
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      req.setAppId(appId);
      
      req.setFromDate(null);
      resp = logService.fetchLogsTillDate(req);
      Assert.assertNotNull(resp);
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      
      req.setFromDate(yesterday.getTime());
      req.setTillDate(null);
      resp = logService.fetchLogsFromDate(req);
      Assert.assertNotNull(resp);
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
}
