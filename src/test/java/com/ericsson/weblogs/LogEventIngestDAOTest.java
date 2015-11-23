/* ============================================================================
*
* FILE: LogEventDAOTest.java
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
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.ericsson.weblogs.dao.LogEventIngestionDAO;
import com.ericsson.weblogs.dao.LogEventRepository;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.domain.LogEventKey;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class LogEventIngestDAOTest {
  
  @Autowired
  private LogEventRepository repo;
  
  LogEvent event;
  List<LogEvent> requests;
  
  @After
  public void delete()
  {
    
    try 
    {
      if (event != null) {
        repo.delete(new BasicMapId().with("appId", appId)/*.with("rownum",
            event.getId().getRownum())*/);
      }
      if(requests != null){
        for(LogEvent l : requests)
        {
          repo.delete(new BasicMapId().with("appId", appId)/*.with("rownum", l.getId().getRownum())*/);
        }
      }
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  final int batchSize = 10;
  final String appId = "applicationId";
  
  @Autowired
  private LogEventIngestionDAO iDao;
  
  @Test
  public void testInsertLogEvents()
  {
    requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      
      requests.add(event);
    }
    try {
      iDao.ingestAsync(requests);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testInsertLogEventsBatch()
  {
        
    requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      
      requests.add(event);
    }
    try {
      iDao.ingestAsyncBatch(requests);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  
  
  @Test
  public void testInsertLogEvent()
  {
    event = new LogEvent();
    event.setId(new LogEventKey());
    event.setLogText("This is some bla blaah bla logging at info level");
    event.getId().setAppId(appId);
    try {
      iDao.insert(event);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }

}
