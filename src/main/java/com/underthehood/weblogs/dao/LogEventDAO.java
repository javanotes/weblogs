/* ============================================================================
*
* FILE: LogEventDAO.java
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
package com.underthehood.weblogs.dao;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.TreeMap;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.security.util.FieldUtils;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.Session;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.domain.LogEventKey;

abstract class LogEventDAO {

  final static String FN_MAXTIMEUUID = "maxTimeuuid";
  final static String FN_MINTIMEUUID = "minTimeuuid";
  final static String FN_TOKEN = "token";
  
  
  @Autowired
  protected CassandraOperations cassandraOperations;
  
  protected static String getColumnForField(Field f) {
    if(f.isAnnotationPresent(PrimaryKeyColumn.class))
    {
      return f.getAnnotation(PrimaryKeyColumn.class).name();
    }
    else
    {
      return f.getAnnotation(Column.class).value();
    }
  }

  protected static boolean isFieldTimeuuid(Field f) {
    return f.getType() == UUID.class;
  }
  protected static boolean isFieldTimestamp(Field f) {
    return f.getType() == Date.class;
  }

  protected final TreeMap<Integer, Field> pkFields = new TreeMap<>();
  protected final TreeMap<Integer, Field> allFields = new TreeMap<>();
  protected String table;

  protected static Object getIfEmbedded(final LogEvent event, final Field f)
      throws IllegalAccessException {
        Class<?> root = LogEvent.class;
        Class<?> declaring = f.getDeclaringClass();
        Object o = event;
        Field decF;
        while(declaring != LogEvent.class)
        {
          decF = ReflectionUtils.findField(root, null, declaring);
          o = FieldUtils.getFieldValue(o, decF.getName());
          root = decF.getType();
          declaring = decF.getDeclaringClass();
        }
        
        return o;
      }

  @PostConstruct
  private void init() {
    pkFields.putAll(orderPKFields());
    allFields.putAll(pkFields);
    getOtherFields(allFields);
    
    table = AnnotationUtils.findAnnotation(LogEvent.class, Table.class)
        .value();
    
    
  }

  @Autowired
  protected Session session;

  private static void getOtherFields(TreeMap<Integer, Field> pkMap) {
    int ord = pkMap.lastKey();
    for(Field f : LogEvent.class.getDeclaredFields())
    {
      if(f.isAnnotationPresent(Column.class))
      {
        pkMap.put(++ord, f);
      }
    }
  }

  private static TreeMap<Integer, Field> orderPKFields() {
    TreeMap<Integer, Field> pkMap = new TreeMap<>();
    PrimaryKeyColumn pk;
    for(Field f : LogEventKey.class.getDeclaredFields())
    {
      if(f.isAnnotationPresent(PrimaryKeyColumn.class))
      {
        pk = f.getAnnotation(PrimaryKeyColumn.class);
        pkMap.put(pk.ordinal(), f);
      }
    }
    return pkMap;
  }

  public LogEventDAO() {
    super();
  }

}