/* ============================================================================
*
* FILE: StandaloneTestSuite.java
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

//import com.datastax.driver.core.utils.UUIDs;
import com.underthehood.weblogs.utils.CommonHelper;
import com.underthehood.weblogs.utils.ConcurrentHeadBuffer;
import com.underthehood.weblogs.utils.TimeuuidGenerator;
import com.underthehood.weblogs.utils.TimeuuidComparator;

@RunWith(BlockJUnit4ClassRunner.class)
public class StandaloneTestSuite {

  @Test
  public void testConcurrentHeadBuffer()
  {

    final ConcurrentHeadBuffer<Integer> b = new ConcurrentHeadBuffer<Integer>(
        3);

    ExecutorService es = Executors.newFixedThreadPool(3);

    for (int i = 0; i < 30; i++) {

      es.submit(new Runnable() {

        @Override

        public void run() {

          Random r = new Random();
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));
          b.add(r.nextInt(129));

        }

      });

    }

    b.add(-1);

    b.add(2);

    b.add(3);
    b.add(11);

    b.add(7);

    b.add(129);

    b.add(11);

    b.add(21);

    b.add(32);
    b.add(112);

    b.add(7);

    es.shutdown();

    try {
      es.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {

      e.printStackTrace();

    }

    
    List<Object> li = Arrays.asList(b.toArray());
    System.out.println();
    Assert.assertEquals(3, li.size());
    Assert.assertEquals(129, (int)li.get(0));
    

  }
  private static long makeEpoch() {
    // UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000 15 Oct 1582.
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
    c.set(Calendar.YEAR, 1582);
    c.set(Calendar.MONTH, Calendar.OCTOBER);
    c.set(Calendar.DAY_OF_MONTH, 15);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTimeInMillis();
  }
  @Test
  public void testGetBetweenDateBuckets()
  {
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.TIMEBUCKET_DATEFORMAT);
    Calendar today = new GregorianCalendar();
    Calendar tomorrow = new GregorianCalendar();
    tomorrow.set(Calendar.DAY_OF_YEAR, today.get(Calendar.DAY_OF_YEAR)+1);
    List<Date> b = CommonHelper.getBetweenDateBuckets(today.getTime(), tomorrow.getTime());
    Assert.assertEquals(2, b.size());
    Assert.assertEquals(sdf.format(today.getTime()), sdf.format(b.get(0)));
    Assert.assertEquals(sdf.format(tomorrow.getTime()), sdf.format(b.get(1)));
    
    tomorrow.set(Calendar.DAY_OF_YEAR, today.get(Calendar.DAY_OF_YEAR)-1);
    b = CommonHelper.getBetweenDateBuckets(today.getTime(), tomorrow.getTime());
    Assert.assertEquals(0, b.size());
    
    b = CommonHelper.getBetweenDateBuckets(tomorrow.getTime(), today.getTime());
    Assert.assertEquals(2, b.size());
    Assert.assertEquals(sdf.format(tomorrow.getTime()), sdf.format(b.get(0)));
    Assert.assertEquals(sdf.format(today.getTime()), sdf.format(b.get(1)));
    
    tomorrow.set(Calendar.DAY_OF_YEAR, today.get(Calendar.DAY_OF_YEAR)+2);
    b = CommonHelper.getBetweenDateBuckets(today.getTime(), tomorrow.getTime());
    Assert.assertEquals(3, b.size());
    Assert.assertEquals(sdf.format(today.getTime()), sdf.format(b.get(0)));
    Assert.assertEquals(sdf.format(tomorrow.getTime()), sdf.format(b.get(2)));
    tomorrow.set(Calendar.DAY_OF_YEAR, tomorrow.get(Calendar.DAY_OF_YEAR)-1);
    Assert.assertEquals(sdf.format(tomorrow.getTime()), sdf.format(b.get(1)));
  }
  //@Test
  /*public void testEpoch()
  {
    Date d = CommonHelper.javaEpochDate();
    UUID u = CommonHelper.javaEpochTimeUUID();
    String s = CommonHelper.makeTimeBasedUUIDString(d.getTime());
    Assert.assertEquals("Epoch do not match", d.getTime(), UUIDs.unixTimestamp(u));
    
  }*/
  @Test
  public void testMurmurHashComparisonFails()
  {
    Date d = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.TIMEBUCKET_DATEFORMAT);
    CassandraMurmurHash hash = new CassandraMurmurHash();
    long prev = -1, next;
    int i;
    for(i=0; i<100; i++)
    {
      d = CommonHelper.addDays(d, 1);
      next = hash.getToken(sdf.format(d));
      if(prev != -1)
      {
        if(next > prev)
          continue;
        else
          break;
      }
      prev = next;
    }
    Assert.assertTrue(i != 100);
  }
  @Test
  public void testMakeDecrementalTimeBasedUUID()
  {
    LinkedList<UUID> uuids = new LinkedList<>();
    Date d = new Date();
    for(int i=0; i<10; i++)
    {
      d = CommonHelper.addDays(d, -1);
      UUID u = CommonHelper.makeTimeUuid(d.getTime());
      Assert.assertEquals("Not correct type 1 uuid", d.getTime(), TimeuuidGenerator.unixTimestamp(u));
      uuids.add(u);
    }
    
    UUID prev = null, next;
    //System.out.println("testMakeTimeBasedUUID:: Total UUIDs created- "+uuids.size());
    //not working
    TimeuuidComparator compare = new TimeuuidComparator();
    for(Iterator<UUID> iter = uuids.iterator(); iter.hasNext();)
    {
      next = iter.next();
      if(prev != null)
      {
        int compared = compare.compare(prev, next);//compare.compare(prev, next);
        Assert.assertTrue("Comparison failed.", compared > 0);
      }
      prev = next;
    }
  }
  @Test
  public void testMakeTimeBasedUUID()
  {
    long ep1 = makeEpoch();
    final long time = System.currentTimeMillis();
    Date d = new Date(time);
    try 
    {
      UUID u = CommonHelper.makeTimeUuid(time);
      Assert.assertNotNull(u);
      Assert.assertEquals("Not correct type 1 uuid", time, TimeuuidGenerator.unixTimestamp(u));
      Assert.assertEquals("Not correct date", time, d.getTime());
      LinkedList<UUID> uuids = new LinkedList<>();
      for(int i=0;i<10;i++)
      {
                
        for (int j = 0; j < 10; j++) {
                    
          u = CommonHelper.makeTimeUuid(time);
          Assert.assertEquals("Incremental time series should match uuid & timestamp", time,
              TimeuuidGenerator.unixTimestamp(u));
          
          uuids.add(u);
          
        }
        
      }
      UUID prev = null, next;
      //System.out.println("testMakeTimeBasedUUID:: Total UUIDs created- "+uuids.size());
      //not working
      TimeuuidComparator compare = new TimeuuidComparator();
      for(Iterator<UUID> iter = uuids.descendingIterator(); iter.hasNext();)
      {
        next = iter.next();
        if(prev != null)
        {
          int compared = compare.compare(prev, next);//compare.compare(prev, next);
          Assert.assertTrue("Comparison failed.", compared > 0);
        }
        prev = next;
      }
      
      u = CommonHelper.makeTimeUuid(time+1);
      Assert.assertEquals("Incremental time series should match uuid & timestamp", time+1,
          TimeuuidGenerator.unixTimestamp(u));
      
      u = CommonHelper.makeTimeUuid(time-1);
      Assert.assertEquals("Decremental time series should match uuid & timestamp", time - 1,
          TimeuuidGenerator.unixTimestamp(u));
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    long ep2 = makeEpoch();
    Assert.assertEquals("Not correct epochs!", ep1, ep2);
  }
  @Test
  public void testHourFillings()
  {
    Calendar c = new GregorianCalendar();
    Date d1 = c.getTime();
    c.add(Calendar.DAY_OF_MONTH, 1);
    Date d2 = c.getTime();
    System.out.println("StandaloneTestSuite.testHourFillings()");
    System.out.println("From: "+d1+"\tTo: "+d2);
    Set<Date> s = CommonHelper.fillBetweenHours(d1, d2);
    System.out.println(s.size());
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.LOG_TREND_HOURLY_FORMAT);
    for(Date d : s)
    {
      System.out.println(sdf.format(d));
    }
    
    c.add(Calendar.HOUR_OF_DAY, 1);
    d1 = c.getTime();
    System.out.println("From: "+d2+"\tTo: "+d1);
    s = CommonHelper.fillBetweenHours(d2, d1);
    System.out.println(s.size());
    for(Date d : s)
    {
      System.out.println(sdf.format(d));
    }
  }
  
  @Test
  public void testDayFillings()
  {
    Calendar c = new GregorianCalendar();
    Date d1 = c.getTime();
    c.add(Calendar.DAY_OF_MONTH, 1);
    Date d2 = c.getTime();
    System.out.println("StandaloneTestSuite.testDayFillings()");
    System.out.println("From: "+d1+"\tTo: "+d2);
    Set<Date> s = CommonHelper.fillBetweenDays(d1, d2);
    System.out.println(s.size());
    SimpleDateFormat sdf = new SimpleDateFormat(CommonHelper.LOG_TREND_DAILY_FORMAT);
    for(Date d : s)
    {
      System.out.println(sdf.format(d));
    }
  }
 
  public void testLuceneFTS()
  {/*
    try {
      FullTextSearch fts = new FullTextSearch();
      Set<String> tokens = fts.tokenizeText("Rama is a good boy. perhaps he was down with fever. Nad also John was down with fever as well."
          + " Perhaps I am trying to make this as long as a logging requires so that the actual story doesnt get boring. "
          + " When he was doing Lorem Ipsum and the quick brown fox ran over the crazy dog. huhh!", Sets.newHashSet("down with fever"), false, false);
      //System.out.println(tokens);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
      
    }
  */}
}
