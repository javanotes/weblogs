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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
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
  
  public static final String ISO8601_DATE_ZONE = "yyyy-MM-ddZ";
  public static final String ISO8601_DATETIME_ZONE = "yyyy-MM-dd HH:mm:ssZ";
  public static final String ISO8601_TS_MILLIS_ZONE = "yyyy-MM-dd HH:mm:ss.SSSZ";
  public static final String ISO8601_TS_MILLIS = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String ISO8601_TST_MILLIS_ZONE = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  public static final String ISO8601_TST_MILLIS = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  
  
  private static Date epochDate;
  
  public static Date javaEpochDate()
  {
    if (epochDate == null) {
      try {
        epochDate = new SimpleDateFormat(ISO8601_TS_MILLIS_ZONE)
            .parse("1970-01-01 00:00:00.000 GMT");
      } catch (ParseException e) {
        throw new RuntimeException(e);
      } 
    }
    return epochDate;
  }
  private static void handleArraysListsPojosAsField(Class<?> _class, final Object o)
  {
    ReflectionUtils.doWithFields(_class, new ReflectionUtils.FieldCallback() {
      
      @SuppressWarnings({ "rawtypes", "unchecked" })
      @Override
      public void doWith(Field field)
          throws IllegalArgumentException, IllegalAccessException {

        
        if(Modifier.isStatic(field.getModifiers()))
          return;
        
        if(!field.isAccessible())
          field.setAccessible(true);
        
        Class<?> fldCls = field.getType();
        Object fld_o = null;
        Class<?> arrTyp;
        
        if(fldCls.isArray())
        {
          fld_o = Array.newInstance(fldCls, 1);
          if(field.getGenericType() instanceof ParameterizedType)
          {
            ParameterizedType pTyp = (ParameterizedType) field.getGenericType();
            arrTyp = (Class<?>) pTyp.getActualTypeArguments()[0];
          }
          else
          {
            arrTyp = (Class<?>) field.getGenericType();
          }
          
          try {
            Array.set(fld_o, 0, instantiateRecursively(arrTyp));
          }  catch (Exception e) {
            throw new IllegalAccessException(e.getMessage());
          }
        }
        else if(Collection.class.isAssignableFrom(fldCls))
        {
          if(field.getGenericType() instanceof ParameterizedType)
          {
            ParameterizedType pTyp = (ParameterizedType) field.getGenericType();
            arrTyp = (Class<?>) pTyp.getActualTypeArguments()[0];
          }
          else
          {
            arrTyp = (Class<?>) field.getGenericType();
          }
          
          if(List.class.isAssignableFrom(fldCls))
          {
            
            if(LinkedList.class.isAssignableFrom(fldCls))
            {
              fld_o = new LinkedList<>();
            }
            else
            {
              fld_o = new ArrayList<>(1);
            }
            
          }
          
          else if(Set.class.isAssignableFrom(fldCls))
          {
            
            if(TreeSet.class.isAssignableFrom(fldCls))
            {
              fld_o = new TreeSet<>();
            }
            else
            {
              fld_o = new HashSet<>(1);
            }
          }
                    
          if (fld_o != null) {
            try {
              ((Collection) fld_o).add(instantiateRecursively(arrTyp));
            } catch (InstantiationException e) {
              throw new IllegalAccessException(e.getMessage());
            } 
          }
        }
        else if (isPojoClass(field.getType())) {
          try 
          {
            fld_o = instantiateRecursively(field.getType());
                        
          } catch (InstantiationException e) {
            throw new IllegalAccessException(e.getMessage());
          }
        }
        
        if (fld_o != null) {
          field.set(o, fld_o);
        }

      
        
      }
    });
  }
  /**
   * Utility method to recursively instantiate a simple POJO type class, which can contain
   * primitives, java.lang.., java.util.., arrays, lists (array/linked), sets (hash/tree)
   * or other simple POJO.
   * @param _class
   * @return
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public static Object instantiateRecursively(Class<?> _class)
      throws InstantiationException, IllegalAccessException {
    final Object o = _class.newInstance();
    if(isPojoClass(_class) || _class.isArray() || Collection.class.isAssignableFrom(_class))
    {
      handleArraysListsPojosAsField(_class, o);
    }
    
    return o;

  }
  
  public static boolean isPojoClass(Class<?> clazz)
  {
    
    String pkg = ClassUtils.getPackageName(clazz);
    return !clazz.isEnum() && !clazz.isAnonymousClass() && !ClassUtils.isPrimitiveOrWrapper(clazz) 
        && !clazz.isArray()
        && !Collection.class.isAssignableFrom(clazz)
        && !Map.class.isAssignableFrom(clazz) && !pkg.startsWith("java.lang") 
        && !pkg.startsWith("java.io") && !pkg.startsWith("java.nio") && !pkg.startsWith("java.net") && !pkg.startsWith("java.util")
        && !pkg.startsWith("java.math") && !pkg.startsWith("java.text") && !pkg.startsWith("java.sql")
        && !pkg.startsWith("java.awt") && !pkg.startsWith("java.security") && !pkg.startsWith("java.applet")
        && !pkg.startsWith("java.beans") && !pkg.startsWith("java.rmi") && !pkg.startsWith("javax.");
  }
  
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
    parser.applyPattern(ISO8601_DATE_ZONE);
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
    parser.applyPattern(ISO8601_DATETIME_ZONE);
    return parser.format(date);
  }
}
