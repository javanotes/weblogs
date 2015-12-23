/* ============================================================================
*
* FILE: LogEventRepositoryTest.java
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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.underthehood.weblogs.dao.LogEventRepository;
import com.underthehood.weblogs.dto.LogEventDTO;
import com.underthehood.weblogs.dto.LogRequest;
import com.underthehood.weblogs.dto.QueryRequest;
import com.underthehood.weblogs.dto.QueryResponse;
import com.underthehood.weblogs.dto.SliceQueryRequest;
import com.underthehood.weblogs.service.ILogAggregationService;
import com.underthehood.weblogs.service.ILoggingService;
import com.underthehood.weblogs.service.ServiceException;
import com.underthehood.weblogs.utils.CommonHelper;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class LogEventServiceTest {
  
  @Autowired
  private ILoggingService logService;
  @Autowired
  private ILogAggregationService logMetrics;
  
  @Autowired
  private LogEventRepository repo;
  
  LogRequest event;
  List<LogRequest> requests;
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
    if(requests != null)
    {
      for(LogRequest r : requests)
      {
        try {
          MapId id = r.toMapId();
          repo.delete(id);
        } catch (Exception e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }
  
  final String EXEC_START = "EXEC_START:";
  final String EXEC_END = "EXEC_END:";
  
  @Test
  public void testCountExecutionTimings()
  {
    requests = new ArrayList<>();
    
    final long execStartTime = System.currentTimeMillis();
    final long execDur = 5;
    
    for(int i=0; i<batchSize; i++)
    {
      event = new LogRequest();
      event.setLogText(EXEC_START+" This is some bla blaah bla logging at info level");
      event.setExecutionId(i+"");
      event.setApplicationId(appId);
      event.setTimestamp(execStartTime+i);
      requests.add(event);
      
    }
    final long execEndTime = event.getTimestamp();
    
    for(int i=0; i<batchSize; i++)
    {
      event = new LogRequest();
      event.setLogText(EXEC_END+" This is some bla blaah bla logging at info level");
      event.setExecutionId(i+"");
      event.setApplicationId(appId);
      event.setTimestamp(execEndTime + (i*execDur));
      requests.add(event);
    }
    
    try 
    {
      logService.ingestLoggingRequests(requests);
      Thread.sleep(1000);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
    SliceQueryRequest req = new SliceQueryRequest();
    req.setAppId(appId);
    req.setFetchSize(batchSize/2);
    req.setFromDate(new Date());
    Calendar tomorrow = GregorianCalendar.getInstance();
    tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
    req.setTillDate(tomorrow.getTime());
    
    req.setSearchTerm(EXEC_START);
    req.setSearchTermEnd(EXEC_END);
    
    try 
    {
      Map<String, Map<Date, Long>> counts = logMetrics.countExecutionTimings(req);
      Assert.assertNotNull(counts);
      Assert.assertFalse(counts.isEmpty());
      Assert.assertEquals(batchSize, counts.size());
      
      int offset = 0;
      
      NavigableMap<String, Map<Date, Long>> sorted = new TreeMap<>(counts);
      //TODO
      for(Entry<String, Map<Date, Long>> e : sorted.entrySet())
      {
        Assert.assertEquals(offset+"", e.getKey());    
        Assert.assertEquals(1, e.getValue().size());
        
        for(Entry<Date, Long> e2 : e.getValue().entrySet())
        {
          Assert.assertEquals("Timestamp mismatch", (execStartTime+offset), e2.getKey().getTime());
          Assert.assertEquals("Timing mismatch", (execEndTime-(execStartTime+offset))+(offset*execDur), e2.getValue().longValue());
        }
                
        offset++;
      }
      
    } catch (ServiceException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testInsertLogEvent()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    event.setTimestamp(System.currentTimeMillis());
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
    
    requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      l = new LogRequest();
      l.setLogText(i+": This is some bla blaah bla logging at info level");
      l.setApplicationId(appId);
      l.setTimestamp(System.currentTimeMillis());
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
  final String LOG_PREFIX = "This is a log prefix with offset- ";
  
  @Test
  public void testLogEventsPagination()
  {
    event = new LogRequest();
    event.setApplicationId(appId);//for cleanup
    LogRequest l;
    requests = new ArrayList<>();
    for(int i=0; i<batchSize*3; i++)
    {
      l = new LogRequest();
      l.setLogText(LOG_PREFIX + i);
      l.setApplicationId(appId);
      l.setTimestamp(System.currentTimeMillis());
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      Thread.sleep(5000);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    req.setFetchSize(batchSize);
    req.setFromDate(new Date());
    Calendar tomorrow = GregorianCalendar.getInstance();
    tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
    req.setTillDate(tomorrow.getTime());
    
    QueryResponse resp;
    
    try 
    {
      //page 1
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(batchSize*3, resp.getITotalDisplayRecords());
      
      int offset = batchSize*3;
      String text;
      for(LogEventDTO log : resp.getLogs())
      {
        text = LOG_PREFIX+(--offset);
        Assert.assertTrue("Log text did not expected", text.equals(log.getLogText()));
      }
      
      //page 2
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(batchSize*3, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = LOG_PREFIX+(--offset);
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      //page 3
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(batchSize*3, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = LOG_PREFIX+(--offset);
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      String prevPageMark = resp.getFirstRowUUID();
      
      //not exists
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNull(resp.getFirstRowUUID());
      Assert.assertNull(resp.getLastRowUUID());
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
      
      req.setFetchPrev(true);
      offset = batchSize*2;
      
      //page 2
      req.setFetchMarkUUID(prevPageMark);
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(batchSize*3, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = LOG_PREFIX+(--offset);
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      offset = batchSize*3;
      //page 1
      req.setFetchMarkUUID(resp.getFirstRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(batchSize*3, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = LOG_PREFIX+(--offset);
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      //not exist
      req.setFetchMarkUUID(resp.getFirstRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNull(resp.getFirstRowUUID());
      Assert.assertNull(resp.getLastRowUUID());
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize*3, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
    } catch (ServiceException e) {
      Assert.fail(e.getMessage());
    }
  }
  
  @Test
  public void testLogEventsPaginationWithTerm()
  {
    String term = "Lorem Ipsum dollar ";
    event = new LogRequest();
    event.setApplicationId(appId);//for cleanup
    LogRequest l;
    requests = new ArrayList<>();
    for(int i=0; i<batchSize*3; i++)
    {
      l = new LogRequest();
      l.setLogText(i % 2 == 0 ? (LOG_PREFIX + i) : (term + LOG_PREFIX + i));
      l.setApplicationId(appId);
      l.setTimestamp(System.currentTimeMillis());
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      Thread.sleep(5000);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    req.setFetchSize(batchSize/2);
    req.setFromDate(new Date());
    req.setSearchTerm(term);
    Calendar tomorrow = GregorianCalendar.getInstance();
    tomorrow.set(Calendar.DATE, tomorrow.get(Calendar.DATE)+1);
    req.setTillDate(tomorrow.getTime());
    
    QueryResponse resp;
    
    try 
    {
      //page 1
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalDisplayRecords());
      
      int offset = batchSize*3;
      String text;
      for(LogEventDTO log : resp.getLogs())
      {
        text = term + LOG_PREFIX+(--offset);
        --offset;
        Assert.assertTrue("Log text did not expected", text.equals(log.getLogText()));
      }
      
      //page 2
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = term + LOG_PREFIX+(--offset);
        --offset;
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      //page 3
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = term + LOG_PREFIX+(--offset);
        --offset;
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      String prevPageMark = resp.getFirstRowUUID();
      
      //not exists
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNull(resp.getFirstRowUUID());
      Assert.assertNull(resp.getLastRowUUID());
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
      
      req.setFetchPrev(true);
      offset = batchSize*2;
      
      //page 2
      req.setFetchMarkUUID(prevPageMark);
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = term + LOG_PREFIX+(--offset);
        --offset;
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      offset = batchSize*3;
      //page 1
      req.setFetchMarkUUID(resp.getFirstRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalDisplayRecords());
      
      for(LogEventDTO log : resp.getLogs())
      {
        text = term + LOG_PREFIX+(--offset);
        --offset;
        Assert.assertTrue("Log text not expected", text.equals(log.getLogText()));
      }
      
      //not exist
      req.setFetchMarkUUID(resp.getFirstRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNull(resp.getFirstRowUUID());
      Assert.assertNull(resp.getLastRowUUID());
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals((batchSize*3)/2, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
    } catch (ServiceException e) {
      Assert.fail(e.getMessage());
    }
  }
  
  @Test 
  public void testFindLogEventsBetweenDates()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      l = new LogRequest();
      l.setLogText(i+": This is some bla blaah bla logging at info level");
      l.setApplicationId(appId);
      l.setTimestamp(System.currentTimeMillis());
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      Thread.sleep(5000);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    req.setFetchSize(batchSize/2);
    
    Calendar yesterday = GregorianCalendar.getInstance();
    yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
    req.setFromDate(yesterday.getTime());
    
    Calendar tomorrow = GregorianCalendar.getInstance();
    tomorrow.set(Calendar.DATE, yesterday.get(Calendar.DATE)+1);
    req.setTillDate(tomorrow.getTime());
    
    QueryResponse resp;
    try 
    {
      //next pages
      req.setFetchPrev(false);
      
      //page 1
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals(batchSize, resp.getITotalRecords());
      Assert.assertEquals(batchSize, resp.getITotalDisplayRecords());
      
      //page 2
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals(batchSize, resp.getITotalRecords());
      Assert.assertEquals(batchSize, resp.getITotalDisplayRecords());
      
      String page2FirstRow = resp.getFirstRowUUID();
      
      //page 3 (not exists)
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
      //prev pages
      req.setFetchPrev(true);
      
      //page 1
      req.setFetchMarkUUID(page2FirstRow);
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals(batchSize, resp.getITotalRecords());
      Assert.assertEquals(batchSize, resp.getITotalDisplayRecords());
      
      //page 0 does not exist
      page2FirstRow = resp.getFirstRowUUID();
      req.setFetchMarkUUID(page2FirstRow);
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
      req.setAppId("doNotMatch");
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
           
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
  }
  @Test
  public void testCountLogLevels()
  {
    //testInsertLogEvents();
    //insert with different terms
    requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      
      l = new LogRequest();
      l.setLogText("This is some bla blaah bla logging at info level: "+(i % 2 == 0 ? "Lorem" : "Ipsum"));
      l.setApplicationId(appId);
      l.setTimestamp(System.currentTimeMillis());
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      Thread.sleep(5000);//for index update
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    req.setFetchSize(batchSize/2);
    req.setLevel("INFO");
    
    Calendar yesterday = GregorianCalendar.getInstance();
    yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
    req.setFromDate(yesterday.getTime());
    
    Calendar today = GregorianCalendar.getInstance();
    req.setTillDate(today.getTime());
    SimpleDateFormat format = new SimpleDateFormat();
    try 
    {
      Map<String, Long> counts = logMetrics.countDailyLogsByLevel(req);
      Assert.assertNotNull(counts);
      Assert.assertFalse(counts.isEmpty());
      Assert.assertEquals(1, counts.keySet().size());
      format.applyPattern(CommonHelper.LOG_TREND_DAILY_FORMAT);
      Assert.assertTrue(counts.containsKey(format.format(today.getTime())));
      long count = counts.get(format.format(today.getTime()));
      Assert.assertEquals((batchSize), count);
      
      counts = logMetrics.countHourlyLogsByLevel(req);
      Assert.assertNotNull(counts);
      Assert.assertFalse(counts.isEmpty());
      Assert.assertEquals(24, counts.keySet().size());
      format.applyPattern(CommonHelper.LOG_TREND_HOURLY_FORMAT);
      Assert.assertTrue(counts.containsKey(format.format(today.getTime())));
      count = counts.get(format.format(today.getTime()));
      Assert.assertEquals((batchSize), count);
      
      req.setSearchTerm("lOREm");
      
      counts = logMetrics.countDailyLogsByLevel(req);
      Assert.assertNotNull(counts);
      Assert.assertFalse(counts.isEmpty());
      Assert.assertEquals(1, counts.keySet().size());
      format.applyPattern(CommonHelper.LOG_TREND_DAILY_FORMAT);
      Assert.assertTrue(counts.containsKey(format.format(today.getTime())));
      count = counts.get(format.format(today.getTime()));
      Assert.assertEquals((batchSize/2), count);
      
      counts = logMetrics.countHourlyLogsByLevel(req);
      Assert.assertNotNull(counts);
      Assert.assertFalse(counts.isEmpty());
      Assert.assertEquals(24, counts.keySet().size());
      format.applyPattern(CommonHelper.LOG_TREND_HOURLY_FORMAT);
      Assert.assertTrue(counts.containsKey(format.format(today.getTime())));
      count = counts.get(format.format(today.getTime()));
      Assert.assertEquals((batchSize/2), count);
      
    } catch (ServiceException e) {
      Assert.fail(e.getMessage());
    }
  }
  @Test
  public void testFindLogEventsBetweenDatesWithTerm()
  {
   
    //insert with different terms
    requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      
      l = new LogRequest();
      l.setLogText("This is some bla blaah bla logging at info level: "+(i % 2 == 0 ? "Lorem" : "Ipsum"));
      l.setApplicationId(appId);
      l.setTimestamp(System.currentTimeMillis());
      requests.add(l);
    }
    try {
      logService.ingestLoggingRequests(requests);
      Thread.sleep(5000);//for index update
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
    
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    req.setFetchSize(batchSize/2);
    req.setSearchTerm("lOREm");
    
    Calendar yesterday = GregorianCalendar.getInstance();
    yesterday.set(Calendar.DATE, yesterday.get(Calendar.DATE)-1);
    req.setFromDate(yesterday.getTime());
    
    Calendar tomorrow = GregorianCalendar.getInstance();
    tomorrow.set(Calendar.DATE, yesterday.get(Calendar.DATE)+1);
    req.setTillDate(tomorrow.getTime());
    
    QueryResponse resp;
    try 
    {
      //next pages
      req.setFetchPrev(false);
      
      //page 1
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals(batchSize/2, resp.getITotalRecords());
      Assert.assertEquals(batchSize/2, resp.getITotalDisplayRecords());
      
      String page2FirstRow = resp.getFirstRowUUID();
      
      //page 2 (not exists)
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize/2, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
            
      //prev pages
      req.setFetchPrev(true);
      
      //page 0 (not exists)
      req.setFetchMarkUUID(page2FirstRow);
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize/2, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
            
      req.setAppId("doNotMatch");
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
      req.setAppId(appId);
      req.setSearchTerm("Ipsum");
      req.setFetchPrev(false);
      req.setFetchMarkUUID(null);
      
      //page 1
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertNotNull(resp.getFirstRowUUID());
      Assert.assertNotNull(resp.getLastRowUUID());
      Assert.assertFalse(resp.getLogs().isEmpty());
      Assert.assertEquals(batchSize/2, resp.getLogs().size());
      Assert.assertEquals(batchSize/2, resp.getITotalRecords());
      Assert.assertEquals(batchSize/2, resp.getITotalDisplayRecords());
      
      page2FirstRow = resp.getFirstRowUUID();
      
      //page 2 (not exists)
      req.setFetchMarkUUID(resp.getLastRowUUID());
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize/2, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
            
      //prev pages
      req.setFetchPrev(true);
      
      //page 0 (not exists)
      req.setFetchMarkUUID(page2FirstRow);
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getLogs().size());
      Assert.assertEquals(batchSize/2, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
      
      req.setAppId("doNotMatch");
      resp = logService.fetchLogsBetweenDates(req);
      Assert.assertNotNull(resp);
      Assert.assertTrue(resp.getLogs().isEmpty());
      Assert.assertEquals(0, resp.getITotalRecords());
      Assert.assertEquals(0, resp.getITotalDisplayRecords());
           
      
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
