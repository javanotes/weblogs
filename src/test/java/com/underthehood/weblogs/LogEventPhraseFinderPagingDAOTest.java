/* ============================================================================
*
* FILE: LogEventDAOTest.java
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
package com.underthehood.weblogs;

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

//import com.datastax.driver.core.utils.UUIDs;
import com.underthehood.weblogs.dao.LogEventFinderPagingDAO;
import com.underthehood.weblogs.dao.LogEventIngestionDAO;
import com.underthehood.weblogs.dao.LogEventRepository;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.utils.CommonHelper;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class LogEventPhraseFinderPagingDAOTest {
  
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
        repo.delete(new BasicMapId().with("appId", appId).with("bucket",
            event.getId().getBucket()));
        repo.delete(new BasicMapId().with("appId", appId+":").with("bucket", event.getId().getBucket()));
      }
      if(requests != null){
        for(LogEvent l : requests)
        {
          repo.delete(new BasicMapId().with("appId", appId).with("bucket", l.getId().getBucket()));
          repo.delete(new BasicMapId().with("appId", appId+":").with("bucket", l.getId().getBucket()));
        }
      }
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
        
  }
  
  final int batchSize = 10;
  final String appId = "applicationId";
  
  @Autowired@Qualifier("mainDAO")
  private LogEventFinderPagingDAO fDao;
  @Autowired
  private LogEventIngestionDAO iDao;
  
  //@Test
  public void testCount()
  {
    
    requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      requests.add(event);
    }
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      Thread.sleep(1000);//for index updates
      
      long count = fDao.count(appId, "", null, null, null);
      Assert.assertEquals("Incorrect count", batchSize, count);
      
      count = fDao.count(appId, "blaah", null, null, null);
      Assert.assertEquals("Incorrect count using term", batchSize, count);
      
      count = fDao.count(appId, "blaah bla logging", null, null, null);
      Assert.assertEquals("Incorrect count using phrase", batchSize, count);
      
      count = fDao.count(appId, "NOTPRESENT", null, null, null);
      Assert.assertEquals("Incorrect count", 0, count);
      
      Calendar yesterday = GregorianCalendar.getInstance();
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      
      Calendar tomorrow = GregorianCalendar.getInstance();
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
      
      count = fDao.count(appId, "", "",tomorrow.getTime(), null);
      Assert.assertEquals("Incorrect count", 0, count);
      
      count = fDao.count(appId, "", "INFO", null, tomorrow.getTime());
      Assert.assertEquals("Incorrect count", batchSize, count);
      
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  //@Test
  public void testCountBetweenDates()
  {
    
    requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      requests.add(event);
    }
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      
     
      Calendar yesterday = GregorianCalendar.getInstance();
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      
      Calendar tomorrow = GregorianCalendar.getInstance();
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
      
      requests.clear();
      
      event = new LogEvent();
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      //event.getId().setTimestamp(UUIDs.endOf(tomorrow.getTimeInMillis()));
      event.getId().setTimestamp(CommonHelper.maxDateUuid(tomorrow.getTime()));
      requests.add(event);
      
      event = new LogEvent();
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      //event.getId().setTimestamp(UUIDs.endOf(yesterday.getTimeInMillis()));
      event.getId().setTimestamp(CommonHelper.maxDateUuid(yesterday.getTime()));
      requests.add(event);
      
      iDao.ingestEntitiesAsync(requests);
      Thread.sleep(1000);//for index updates
      
      long count = fDao.count(appId, "", "", yesterday.getTime(), tomorrow.getTime());
      Assert.assertEquals("Incorrect count for between: ", batchSize+2, count);
      
      count = fDao.count(appId, "blaah bla logging", "", yesterday.getTime(), tomorrow.getTime());
      Assert.assertEquals("Incorrect count for between using phrase: ", batchSize+2, count);
      
      count = fDao.count(appId, "logging", "", yesterday.getTime(), tomorrow.getTime());
      Assert.assertEquals("Incorrect count for between using term: ", batchSize+2, count);
      
      count = fDao.count(appId, "", "INFO", yesterday.getTime(), null);
      Assert.assertEquals("Incorrect count for after: ", batchSize+1, count);
      
      count = fDao.count(appId, "blaah bla logging", "", yesterday.getTime(), null);
      Assert.assertEquals("Incorrect count for after using phrase: ", batchSize+1, count);
      
      count = fDao.count(appId, "blaah", "INFO", yesterday.getTime(), null);
      Assert.assertEquals("Incorrect count for after using term: ", batchSize+1, count);
      
      count = fDao.count(appId, "", "INFO", null, tomorrow.getTime());
      Assert.assertEquals("Incorrect count for before: ", batchSize+1, count);
      
      count = fDao.count(appId, "blaah bla logging", "INFO", null, tomorrow.getTime());
      Assert.assertEquals("Incorrect count for before using phrase: ", batchSize+1, count);
      
      count = fDao.count(appId, "bla", "INFO", null, tomorrow.getTime());
      Assert.assertEquals("Incorrect count for before using term: ", batchSize+1, count);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testFindByAppIdWithTermAndDate()
  {
        
    requests = new ArrayList<>(batchSize);
    Calendar yesterday = GregorianCalendar.getInstance();
    yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
    long today = System.currentTimeMillis();
    
    final String appId1 = appId+":";
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setLogText((i % 2 == 0) ? "This is some bla blaah bla logging at info level" : "quick brown fox Lorem Ipsum do re mi ");
      event.getId().setAppId((i % 2 == 0) ? appId : appId1);
      //event.getId().setTimestamp((i % 2 == 0) ? UUIDs.endOf(yesterday.getTimeInMillis()) : UUIDs.timeBased());
      event.getId().setTimestamp((i % 2 == 0) ? CommonHelper.maxDateUuid(yesterday.getTime()) : CommonHelper.makeTimeUuid());
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      //event.setTokens(fts.tokenizeText(event.getLogText()));
      requests.add(event);
    }
    
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      Thread.sleep(1000);//for index update
      
      /*List<LogEvent> page1 = fDao.findByAppIdAfterDateContains(appId1, "Lorem Ipsum", null, yesterday.getTime(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size after, ", 3, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      page1 = fDao.findByAppIdAfterDateContains(appId1, "Lorem Ipsum", last, yesterday.getTime(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size after, ", 2, page1.size());
      
      last = page1.get(0);
      page1 = fDao.findByAppIdAfterDateContains(appId1, "Lorem Ipsum", last, yesterday.getTime(), 3, true);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size after, ", 3, page1.size());
      
      page1 = fDao.findByAppIdBeforeDateContains(appId1, "Lorem Ipsum", null, new Date(today), batchSize, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertTrue("Incorrect resultset size for no match, ", page1.isEmpty());*/
      
      List<LogEvent> page1 = fDao.findByAppIdBetweenDatesContains(appId1, "Lorem Ipsum", "", null, yesterday.getTime(), new Date(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size between, ", 3, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      page1 = fDao.findByAppIdBetweenDatesContains(appId1, "Lorem Ipsum", "INFO", last, yesterday.getTime(), new Date(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size between, ", 2, page1.size());
      //previous page
      last = page1.get(0);
      page1 = fDao.findByAppIdBetweenDatesContains(appId1, "Lorem Ipsum", "INFO", last, yesterday.getTime(), new Date(), 3, true);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size between, ", 3, page1.size());
     
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    
  }
  
}
