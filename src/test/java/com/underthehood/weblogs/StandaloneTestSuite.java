/* ============================================================================
*
* FILE: StandaloneTestSuite.java
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
package com.underthehood.weblogs;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.underthehood.weblogs.domain.LogEventKey;
import com.underthehood.weblogs.utils.CommonHelper;

@RunWith(BlockJUnit4ClassRunner.class)
public class StandaloneTestSuite {

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
  public void testMakeTimeBasedUUID()
  {
    long ep1 = makeEpoch();
    final long time = System.currentTimeMillis();
    Date d = new Date(time);
    try 
    {
      LogEventKey k = new LogEventKey();
      UUID u = CommonHelper.makeTimeBasedUUID(time);
      k.setTimestamp(u);
      Assert.assertNotNull(u);
      Assert.assertEquals("Not correct type 1 uuid", time, k.getTimestampAsLong());
      Assert.assertEquals("Not correct date", time, d.getTime());
      for(int i=1;i<10;i++)
      {
                
        for (int j = 0; j < 10; j++) {
                    
          k = new LogEventKey();
          k.setTimestamp(CommonHelper.makeTimeBasedUUID(time));
          Assert.assertEquals("Incremental time series should match uuid & timestamp", time,
              k.getTimestampAsLong());
          
        }
        
      }
      
      k = new LogEventKey();
      k.setTimestamp(CommonHelper.makeTimeBasedUUID(time+1));
      Assert.assertEquals("Incremental time series should match uuid & timestamp", time+1,
          k.getTimestampAsLong());
      
      k = new LogEventKey();
      k.setTimestamp(CommonHelper.makeTimeBasedUUID(time - 1));
      Assert.assertNotEquals("Decremental time series should mismatch uuid & timestamp", time - 1,
          k.getTimestampAsLong());
      
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
  @Test
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
