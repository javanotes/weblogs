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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
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

import com.ericsson.weblogs.dao.LogEventFinderDAO;
import com.ericsson.weblogs.dao.LogEventIngestionDAO;
import com.ericsson.weblogs.dao.LogEventRepository;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.domain.LogEventKey;
import com.ericsson.weblogs.lucene.FullTextSearch;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class LogEventDAOTest {
  
  @Autowired
  private LogEventRepository repo;
  
  LogEvent event;
  @After
  public void delete()
  {
    
    try {
      repo.delete(new BasicMapId().with("appId", appId));
      repo.delete(new BasicMapId().with("appId", appId+":"));
      
      //repo.delete(new BasicMapId().with("appId", appId).with("bucket", bucket));
      //repo.delete(new BasicMapId().with("appId", appId+":").with("bucket", bucket));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  final int batchSize = 10;
  final String appId = "applicationId";
  
  @Autowired
  private LogEventFinderDAO fDao;
  @Autowired
  private LogEventIngestionDAO iDao;
  
  @Test
  public void testFindByAppId()
  {
        
    List<LogEvent> requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId((i % 2 == 0) ? appId : appId+":");
      requests.add(event);
    }
    try 
    {
      iDao.ingestAsync(requests);
      
      List<LogEvent> events = fDao.findByAppId(appId+":");
      Assert.assertNotNull("Found null", events);
      Assert.assertEquals("Incorrect resultset size", batchSize/2, events.size());
      
      events = fDao.findByAppId(appId);
      Assert.assertNotNull("Found null", events);
      Assert.assertEquals("Incorrect resultset size", batchSize/2, events.size());
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
 
  @Autowired
  private FullTextSearch fts;
  
  
  @Test
  public void testFindByAppIdWithTermAndDate()
  {
        
    List<LogEvent> requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId((i % 2 == 0) ? appId : appId+":");
      event.setTokens(fts.tokenizeText("This is some bla blaah bla logging at info level"));
      requests.add(event);
    }
    try 
    {
      iDao.ingestAsync(requests);
      
      Calendar yesterday = GregorianCalendar.getInstance();
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      
      List<LogEvent> events = fDao.findByAppIdAfterDateContains(appId, "bla", yesterday.getTime());
      Assert.assertNotNull("Found null", events);
      Assert.assertEquals("Incorrect resultset size", batchSize/2, events.size());
      
      events = fDao.findByAppIdAfterDateContains(appId, "NOTPRESENT", yesterday.getTime());
      Assert.assertNotNull("Found null", events);
      Assert.assertTrue("False resultset returned", events.isEmpty());
      
      events = fDao.findByAppIdBeforeDateContains(appId, "bla", yesterday.getTime());
      Assert.assertNotNull("Found null", events);
      Assert.assertTrue("False resultset returned", events.isEmpty());
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testFindByAppIdWithTermAndDateBetween()
  {
        
    List<LogEvent> requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId((i % 2 == 0) ? appId : appId+":");
      event.setTokens(fts.tokenizeText("This is some bla blaah bla logging at info level"));
      requests.add(event);
    }
    try 
    {
      iDao.ingestAsync(requests);
      
      Calendar yesterday = GregorianCalendar.getInstance();
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      
      Calendar tomorrow = GregorianCalendar.getInstance();
      tomorrow.set(Calendar.DATE, yesterday.get(Calendar.DATE)+1);
      
      List<LogEvent> events = fDao.findByAppIdBetweenDatesContains(appId, "bla", yesterday.getTime(), tomorrow.getTime());
      Assert.assertNotNull("Found null", events);
      Assert.assertEquals("Incorrect resultset size", batchSize/2, events.size());
      
      events = fDao.findByAppIdBetweenDatesContains(appId, "NOTPRESENT", yesterday.getTime(), tomorrow.getTime());
      Assert.assertNotNull("Found null", events);
      Assert.assertTrue("False resultset returned", events.isEmpty());
      
      events = fDao.findByAppIdBetweenDatesContains(appId, "bla", yesterday.getTime(), new Date());
      Assert.assertNotNull("Found null", events);
      Assert.assertEquals("Incorrect resultset size", batchSize/2, events.size());
      
      events = fDao.findByAppIdBetweenDatesContains(appId, "bla", new Date(), tomorrow.getTime());
      Assert.assertNotNull("Found null", events);
      Assert.assertTrue("False resultset returned", events.isEmpty());
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testInsertLogEvents()
  {
        
    List<LogEvent> requests = new ArrayList<>(batchSize);
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
        
    List<LogEvent> requests = new ArrayList<>(batchSize);
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
