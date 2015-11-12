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
    event.setBucket(bucket);
    
    try {
      logService.ingestLoggingRequest(event);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
  final int batchSize = 10;
  final String appId = "applicationId", bucket = "bucket";
  @Test
  public void testInsertLogEvents()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    event.setBucket(bucket);
    
    List<LogRequest> requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      l = new LogRequest();
      l.setLogText("This is some bla blaah bla logging at info level");
      l.setApplicationId(appId);
      l.setBucket(bucket);
      
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
