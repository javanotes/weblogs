/* ============================================================================
*
* FILE: CommonHelper.java
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
package com.ericsson.weblogs.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.springframework.util.StringUtils;

public class CommonHelper {

  /*
   * Ref: http://docs.datastax.com/en/cql/3.3/cql/cql_reference/timestamp_type_r.html
   */
  public static final String[] CASSANDRA_TS_PATTERNS = new String[] {
      "yyyy-mm-dd HH:mm",
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd HH:mmZ",
      "yyyy-MM-dd HH:mm:ssZ",
      "yyyy-MM-dd'T'HH:mm",
      "yyyy-MM-dd'T'HH:mmZ",
      "yyyy-MM-dd'T'HH:mm:ss",
      "yyyy-MM-dd'T'HH:mm:ssZ",
      "yyyy-MM-dd",
      "yyyy-MM-ddZ"
  };
  
  public static final String ISO8601_TS_MILLIS_ZONE = "yyyy-MM-dd HH:mm:ss.SSSZ";
  public static final String ISO8601_TS_MILLIS = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String ISO8601_TST_MILLIS_ZONE = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  public static final String ISO8601_TST_MILLIS = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  
  public static boolean isCollectionEmptyOrNull(Collection<?> coll)
  {
    if(coll == null || coll.isEmpty())
      return true;
    for(Object o : coll)
    {
      if(!StringUtils.isEmpty(o))
        return false;
    }
    return true;
  }
  /**
   * 
   * @param dateString
   * @return
   * @throws ParseException
   */
  public static Date parseAsCassandraTimestamp(String dateString) throws ParseException
  {
    SimpleDateFormat parser = new SimpleDateFormat();
    parser.setLenient(false);
    for(String pattern : CASSANDRA_TS_PATTERNS)
    {
      parser.applyPattern(pattern);
      try 
      {
        return parser.parse(dateString);
      } catch (ParseException e) {
        
      }
    }
    throw new ParseException("Unable to parse as date ["+dateString+"]", -1);
  }
  /**
   * Formats as a date only string
   * @param date
   * @return
   */
  public static String formatAsCassandraDate(Date date)
  {
    SimpleDateFormat parser = new SimpleDateFormat();
    parser.setLenient(false);
    parser.applyPattern("yyyy-MM-ddZ");
    return parser.format(date);
  }
  /**
   * Formats as a date time string till mins
   * @param date
   * @return
   */
  public static String formatAsCassandraDateTime(Date date)
  {
    SimpleDateFormat parser = new SimpleDateFormat();
    parser.setLenient(false);
    parser.applyPattern("yyyy-MM-dd HH:mmZ");
    return parser.format(date);
  }
}
