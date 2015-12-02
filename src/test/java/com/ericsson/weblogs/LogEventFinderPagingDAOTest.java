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

import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.weblogs.dao.LogEventFinderPagingDAO;
import com.ericsson.weblogs.dao.LogEventIngestionDAO;
import com.ericsson.weblogs.dao.LogEventRepository;
import com.ericsson.weblogs.domain.LogEvent;
import com.ericsson.weblogs.domain.LogEventKey;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class LogEventFinderPagingDAOTest {
  
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
        repo.delete(new BasicMapId().with("appId", appId+":")/*.with("rownum",
            event.getId().getRownum())*/);
      }
      if(requests != null){
        for(LogEvent l : requests)
        {
          repo.delete(new BasicMapId().with("appId", appId)/*.with("rownum", l.getId().getRownum())*/);
          repo.delete(new BasicMapId().with("appId", appId+":")/*.with("rownum", l.getId().getRownum())*/);
        }
      }
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
        
  }
  
  final int batchSize = 10;
  final String appId = "applicationId";
  
  @Autowired
  private LogEventFinderPagingDAO fDao;
  @Autowired
  private LogEventIngestionDAO iDao;
  
  @Test
  public void testFindByAppId()
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
    try 
    {
      iDao.ingestAsync(requests);
      
      List<LogEvent> page1 = fDao.findByAppId(appId, null, batchSize/2, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size 1st page.", batchSize/2, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      
      List<LogEvent> page2 = fDao.findByAppId(appId, last, batchSize/2, false);
      Assert.assertNotNull("Found null", page2);
      Assert.assertEquals("Incorrect resultset size 2nd page. ", batchSize/2, page2.size());
      
      for(LogEvent l : page1)
      {
        Assert.assertFalse("Duplicate data in pages ", page2.contains(l));
      }
      for(LogEvent l : page2)
      {
        Assert.assertFalse("Duplicate data in pages ", page1.contains(l));
      }
      
      last = page2.get(0);
      
      List<LogEvent> page11 = fDao.findByAppId(appId, last, batchSize/2, true);
      Assert.assertNotNull("Found null", page11);
      Assert.assertEquals("Incorrect resultset size prev page. ", batchSize/2, page11.size());
      
      for(LogEvent l : page1)
      {
        Assert.assertTrue("Prev page browsing incorrect ", page11.contains(l));
      }
      for(LogEvent l : page11)
      {
        Assert.assertTrue("Prev page browsing incorrect ", page1.contains(l));
      }
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testFindByAppIdAfterDate()
  {
    
    requests = new ArrayList<>(batchSize);
    Calendar tomorrow = GregorianCalendar.getInstance();
    
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
      event.getId().setTimestamp(UUIDs.endOf(tomorrow.getTimeInMillis()));
            
      requests.add(event);
    }
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      
      Calendar after5Days = GregorianCalendar.getInstance();
      after5Days.set(Calendar.DATE, after5Days.get(Calendar.DATE)+5);
      
      List<LogEvent> page1 = fDao.findByAppIdAfterDate(appId, null, after5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size 1st page.", batchSize/2, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      
      Calendar lastDateCalendar = new GregorianCalendar();
      lastDateCalendar.setTimeInMillis(UUIDs.unixTimestamp(last.getId().getTimestamp()));
      
      Assert.assertEquals(after5Days.get(Calendar.DAY_OF_YEAR)+1, lastDateCalendar.get(Calendar.DAY_OF_YEAR));
      
      List<LogEvent> page2 = fDao.findByAppIdAfterDate(appId, last, after5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page2);
      Assert.assertTrue("Should be empty 2nd page. ", page2.isEmpty());
           
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testFindByAppIdBeforeDate()
  {
    
    requests = new ArrayList<>(batchSize);
    Calendar tomorrow = GregorianCalendar.getInstance();
    
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)-1);
      event.getId().setTimestamp(UUIDs.endOf(tomorrow.getTimeInMillis()));
            
      requests.add(event);
    }
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      
      Calendar before5Days = GregorianCalendar.getInstance();
      before5Days.set(Calendar.DATE, before5Days.get(Calendar.DATE)-5);
      
      List<LogEvent> page1 = fDao.findByAppIdBeforeDate(appId, null, before5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size 1st page.", batchSize/2, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      
      Calendar lastDateCalendar = new GregorianCalendar();
      lastDateCalendar.setTimeInMillis(UUIDs.unixTimestamp(last.getId().getTimestamp()));
      
      Assert.assertEquals(before5Days.get(Calendar.DAY_OF_YEAR)-5, lastDateCalendar.get(Calendar.DAY_OF_YEAR));
      
      List<LogEvent> page2 = fDao.findByAppIdBeforeDate(appId, last, before5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page2);
      Assert.assertTrue("Should be empty 2nd page. ", page2.isEmpty());
           
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testFindByAppIdBetweenDates()
  {
    
    requests = new ArrayList<>(batchSize);
    Calendar tomorrow = GregorianCalendar.getInstance();
    
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)-1);
      event.getId().setTimestamp(UUIDs.endOf(tomorrow.getTimeInMillis()));
            
      requests.add(event);
    }
    tomorrow.setTime(new Date());
    
    for(int i=0; i<batchSize; i++)
    {
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
      event.getId().setTimestamp(UUIDs.endOf(tomorrow.getTimeInMillis()));
            
      requests.add(event);
    }
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      
      Calendar before5Days = GregorianCalendar.getInstance();
      before5Days.set(Calendar.DATE, before5Days.get(Calendar.DATE)-5);
      
      Calendar after5Days = GregorianCalendar.getInstance();
      after5Days.set(Calendar.DATE, after5Days.get(Calendar.DATE)+5);
      
      List<LogEvent> page1 = fDao.findByAppIdBetweenDates(appId, null, before5Days.getTime(), after5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size 1st page.", batchSize/2, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      
      Calendar lastDateCalendar = new GregorianCalendar();
      lastDateCalendar.setTimeInMillis(UUIDs.unixTimestamp(last.getId().getTimestamp()));
      
      Calendar today = new GregorianCalendar();
      Assert.assertEquals(today.get(Calendar.DAY_OF_YEAR)+1, lastDateCalendar.get(Calendar.DAY_OF_YEAR));
      
      List<LogEvent> page2 = fDao.findByAppIdBetweenDates(appId, last, before5Days.getTime(), after5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page2);
      Assert.assertEquals("Incorrect resultset size 1st page.", batchSize/2, page2.size());
           
      for(LogEvent l : page1)
      {
        Assert.assertFalse("Duplicate data in pages ", page2.contains(l));
      }
      for(LogEvent l : page2)
      {
        Assert.assertFalse("Duplicate data in pages ", page1.contains(l));
      }
      
      last = page2.get(page2.size()-1);
      lastDateCalendar = new GregorianCalendar();
      lastDateCalendar.setTimeInMillis(UUIDs.unixTimestamp(last.getId().getTimestamp()));
      
      Assert.assertEquals(today.get(Calendar.DAY_OF_YEAR)-5, lastDateCalendar.get(Calendar.DAY_OF_YEAR));
      
      page2 = fDao.findByAppIdBetweenDates(appId, last, before5Days.getTime(), after5Days.getTime(), batchSize/2, false);
      Assert.assertNotNull("Found null", page2);
      Assert.assertTrue("Should be empty 3rd page. ", page2.isEmpty());
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testCount()
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
    try 
    {
      iDao.ingestAsync(requests);
      
      long count = fDao.count(appId, "", null, null);
      Assert.assertEquals("Incorrect count", batchSize, count);
      
      count = fDao.count(appId, "NOTPRESENT", null, null);
      Assert.assertEquals("Incorrect count", 0, count);
      
      Calendar yesterday = GregorianCalendar.getInstance();
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      
      Calendar tomorrow = GregorianCalendar.getInstance();
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
      
      count = fDao.count(appId, "", tomorrow.getTime(), null);
      Assert.assertEquals("Incorrect count", 0, count);
      
      count = fDao.count(appId, "", null, tomorrow.getTime());
      Assert.assertEquals("Incorrect count", batchSize, count);
      
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  
  @Test
  public void testCountBetweenDates()
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
    try 
    {
      iDao.ingestAsync(requests);
      
     
      Calendar yesterday = GregorianCalendar.getInstance();
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      
      Calendar tomorrow = GregorianCalendar.getInstance();
      tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
      
      requests.clear();
      
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      event.getId().setTimestamp(UUIDs.endOf(tomorrow.getTimeInMillis()));
      requests.add(event);
      
      event = new LogEvent();
      event.setId(new LogEventKey());
      event.setLogText("This is some bla blaah bla logging at info level");
      event.getId().setAppId(appId);
      event.getId().setTimestamp(UUIDs.endOf(yesterday.getTimeInMillis()));
      requests.add(event);
      
      iDao.ingestEntitiesAsync(requests);
      
      long count = fDao.count(appId, "", yesterday.getTime(), tomorrow.getTime());
      Assert.assertEquals("Incorrect count for between: ", batchSize+2, count);
      
      count = fDao.count(appId, "", yesterday.getTime(), null);
      Assert.assertEquals("Incorrect count for after: ", batchSize+1, count);
      
      count = fDao.count(appId, "", null, tomorrow.getTime());
      Assert.assertEquals("Incorrect count for before: ", batchSize+1, count);
      
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
      event.setId(new LogEventKey());
      event.setLogText((i % 2 == 0) ? "This is some bla blaah bla logging at info level" : "Lorem Ipsum");
      event.getId().setAppId((i % 2 == 0) ? appId : appId1);
      event.getId().setTimestamp((i % 2 == 0) ? UUIDs.endOf(yesterday.getTimeInMillis()) : UUIDs.timeBased());
      yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
      //event.setTokens(fts.tokenizeText(event.getLogText()));
      requests.add(event);
    }
    
    try 
    {
      iDao.ingestEntitiesAsync(requests);
      Thread.sleep(1000);//for index update
      List<LogEvent> page1 = fDao.findByAppIdAfterDateContains(appId1, "lorem", null, yesterday.getTime(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size after, ", 3, page1.size());
      
      LogEvent last = page1.get(page1.size()-1);
      page1 = fDao.findByAppIdAfterDateContains(appId1, "lorem", last, yesterday.getTime(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size after, ", 2, page1.size());
      
      last = page1.get(0);
      page1 = fDao.findByAppIdAfterDateContains(appId1, "lorem", last, yesterday.getTime(), 3, true);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size after, ", 3, page1.size());
      
      page1 = fDao.findByAppIdBeforeDateContains(appId1, "lorem", null, new Date(today), batchSize, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertTrue("Incorrect resultset size for no match, ", page1.isEmpty());
      
      page1 = fDao.findByAppIdBetweenDatesContains(appId1, "lorem", null, yesterday.getTime(), new Date(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size between, ", 3, page1.size());
      
      last = page1.get(page1.size()-1);
      page1 = fDao.findByAppIdBetweenDatesContains(appId1, "lorem", last, yesterday.getTime(), new Date(), 3, false);
      Assert.assertNotNull("Found null", page1);
      Assert.assertEquals("Incorrect resultset size between, ", 2, page1.size());
     
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    
  }
  
}
