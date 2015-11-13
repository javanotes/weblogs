/* ============================================================================
*
* FILE: LogEventIngestionDAO.java
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
package com.ericsson.weblogs.dao;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.data.cassandra.core.WriteListener;
import org.springframework.security.util.FieldUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.ericsson.weblogs.domain.LogEvent;

import lombok.extern.slf4j.Slf4j;

@Repository@Slf4j
public class LogEventIngestionDAO extends LogEventDAO {
  
  @PostConstruct
  private void init() {
   
    String cqlIngest = prepareInsertQuery();
    cqlIngestStmt = session.prepare(cqlIngest);
    cqlIngestStmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE);
    log.info(">>>>>>> Prepared ingestion query: "+cqlIngest);
  }
  String prepareInsertQuery()
  {
    
    String qry = "insert into " + table + "( ";
    String args = " values " + "(";
    
    for(Entry<Integer, Field> entry : allFields.entrySet())
    {
      qry += getColumnForField(entry.getValue()) + ",";
      args += isFieldTimeuuid(entry.getValue()) ? "now()," : "?,";
      
    }
    
    if(qry.endsWith(","))
    {
      qry = qry.substring(0, qry.lastIndexOf(','));
    }
    if(args.endsWith(","))
    {
      args = args.substring(0, args.lastIndexOf(','));
    }
        
    qry += ")";
    args += ")";
    
    return qry + args;
    
  }
 
  private PreparedStatement cqlIngestStmt;
  
  private List<Object> bindParams(LogEvent event)
  {
    List<Object> param = new ArrayList<>();
    Object o;
    for(Entry<Integer, Field> entry : allFields.entrySet())
    {
      Field f = entry.getValue();
      
      if(isFieldTimeuuid(f))
        continue;
      
      try 
      {
        
        o = getIfEmbedded(event, f);
        o = FieldUtils.getFieldValue(o, f.getName());
        param.add(o);
        
      } catch (Exception e) {
        log.error("Unable to bind parameters!", e);
      }
      
    }
    return param;
  }
  /**
   * 
   * This is an asynchronous operation for saving as entities. We do not explicitly create the CQL here. 
   * <b>Note:</b> this method will NOT use server generated timeuuid using now() function.
   * @deprecated - Use {@link #ingestAsync(List)} instead.
   * @param entities
   * @throws DataAccessException 
   * 
   */
  public void ingestEntitiesAsync(List<LogEvent> entities) throws DataAccessException
  {
    final DataAccessException dax = new DataAccessException("Async operation had errors");    
    cassandraOperations.insertAsynchronously(entities, new WriteListener<LogEvent>() {

      @Override
      public void onWriteComplete(Collection<LogEvent> entities) {
        synchronized (dax) {
          dax.notify();
          dax.setDidNotify(true);
        }
        
      }

      @Override
      public void onException(Exception x) {
        dax.initCause(x);
        synchronized (dax) {
          dax.notify();
          dax.setDidNotify(true);
        }
      }
    }, new WriteOptions(ConsistencyLevel.ONE, RetryPolicy.DEFAULT));
    
    synchronized (dax) {
      if(!dax.isDidNotify())
      {
        try {
          dax.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("", e);
        }
      }
    }
    if(dax.isHasInitCause())
      throw dax;
  }
  
  /**
   * This is an insert operation designed for high performance writes. A CQL is used to create a PreparedStatement once, 
   * then all row values are bound to the single PreparedStatement and executed asynchronously each, against the Session. <p>
   * This method will use server generated timeuuid using now() function.
   * @param entities
   * @throws DataAccessException
   */
  public void ingestAsync(List<LogEvent> entities) throws DataAccessException
  {
      
    try 
    {
      Assert.notNull(entities);
      Object[] args;
      List<Object> param;
      
      final List<ResultSetFuture> futures = new ArrayList<>();
      final BatchDataAccessException dax = new BatchDataAccessException("Ingest async had errors");
      long start = System.currentTimeMillis();
      log.info("=================== ingestAsync:Starting ingestion batch ===================");
      for(LogEvent event : entities)
      {
        param = bindParams(event);
        args = new Object[param.size()];
        
        futures.add(cassandraOperations.executeAsynchronously(cqlIngestStmt.bind(param.toArray(args))));
        
        if (log.isDebugEnabled()) {
          log.debug(">>>>>>>>>> Sending ingestion params => " + param);
        }
      }
      
      for(ResultSetFuture f : futures)
      {
        try {
          f.getUninterruptibly();
        } catch (Exception e) {
          dax.getExceptions().add(new DataAccessException(e));
        }
      }
      long time = System.currentTimeMillis() - start;
      log.info("=================== ingestAsync:End ingestion batch ===================");
      long secs = TimeUnit.MILLISECONDS.toSeconds(time);
      log.info("Time taken: "+secs+" secs "+(time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
      if(!dax.getExceptions().isEmpty())
        throw dax;
      
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }
  /**
   * This is an insert operation designed for high performance writes. A CQL is used to create a PreparedStatement once, 
   * then all row values are bound to the single PreparedStatement and executed asynchronously and <b>atomically</b> 
   * in BATCH, against the Session. <p>
   * This method will use server generated timeuuid using now() function.
   * @param entities
   * @throws DataAccessException
   */
  public void ingestAsyncBatch(List<LogEvent> entities) throws DataAccessException
  {
      
    try 
    {
      Assert.notNull(entities);
      Object[] args;
      List<Object> param;
      
      BatchStatement batch = new BatchStatement(Type.LOGGED);
      long start = System.currentTimeMillis();
      log.info("=================== ingestAsyncBatch:Starting ingestion batch (logged) ===================");
      for(LogEvent event : entities)
      {
        param = bindParams(event);
        args = new Object[param.size()];
                
        batch.add(cqlIngestStmt.bind(param.toArray(args)));
        
        if (log.isDebugEnabled()) {
          log.debug(">>>>>>>>>> Sending ingestion params => " + param);
        }
      }
      
      session.executeAsync(batch).getUninterruptibly();
      
      long time = System.currentTimeMillis() - start;
      log.info("=================== ingestAsyncBatch:End ingestion batch (logged) ===================");
      long secs = TimeUnit.MILLISECONDS.toSeconds(time);
      log.info("Time taken: "+secs+" secs "+(time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
      
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }
    
  /**
   * Inserts a single log event
   * @param event
   * @throws DataAccessException
   */
  public void insert(LogEvent event) throws DataAccessException
  {
        
    try 
    {
      Assert.notNull(event);
      List<Object> param = bindParams(event);
      if (log.isDebugEnabled()) {
        log.debug(">>>>>>>>>> Sending ingestion params => " + param);
      }
      Object[] args = new Object[param.size()];
      cassandraOperations.execute(cqlIngestStmt.bind(param.toArray(args)));
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }
    
}
