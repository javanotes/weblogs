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
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.datastax.driver.core.utils.UUIDs;
import com.underthehood.weblogs.utils.CommonHelper;

@RunWith(BlockJUnit4ClassRunner.class)
public class StandaloneTestSuite {

  @Test
  public void testMakeTimeBasedUUID()
  {
    long time = System.currentTimeMillis();
    Date d = new Date(time);
    try {
      UUID u = CommonHelper.makeTimeBasedUUID(time);
      Assert.assertNotNull(u);
      Assert.assertEquals("Not correct type 1 uuid", time, UUIDs.unixTimestamp(u));
      Assert.assertEquals("Not correct date", time, d.getTime());
      UUID u2 = CommonHelper.makeTimeBasedUUID(time);
      Assert.assertNotNull(u2);
      Assert.assertEquals("Not correct type 1 uuid", time, UUIDs.unixTimestamp(u2));
      
      Assert.assertNotEquals(u, u2);
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
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
