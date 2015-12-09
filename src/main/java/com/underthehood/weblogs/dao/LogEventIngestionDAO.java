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
package com.underthehood.weblogs.dao;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.springframework.security.util.FieldUtils;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.utils.UUIDs;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;

@Repository@Slf4j
public class LogEventIngestionDAO extends LogEventDAO {
  
  @PostConstruct
  private void init() {
   
    String cqlIngest = prepareInsertQuery(true);
    cqlIngestStmt = session.prepare(cqlIngest);
    cqlIngestStmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE);
    log.debug(">>>>>>> Prepared ingestion query: "+cqlIngest);
    
    cqlIngest = prepareInsertQuery(false);
    cqlIngestEntityStmt = session.prepare(cqlIngest);
    cqlIngestEntityStmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ONE);
    log.debug(">>>>>>> Prepared entity ingestion query: "+cqlIngest);
    
  }
  private String prepareInsertQuery(boolean useServerFunctions)
  {
    
    String qry = "insert into " + table + "( ";
    String args = " values " + "(";
    
    for(Entry<Integer, Field> entry : allFields.entrySet())
    {
      qry += getColumnForField(entry.getValue()) + ",";
      
      // for auto generated timestamp/timeuuid primary keys
      args += useServerFunctions ? (isFieldTimestamp(entry.getValue()) ? "dateof(now()),"
          : isFieldTimeuuid(entry.getValue()) ? "now()," : "?,") : "?,";

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
 
  private PreparedStatement cqlIngestStmt,cqlIngestEntityStmt;
  
  private List<Object> bindParams(LogEvent event)
  {
    return bindParams(event, true);
  }
  private List<Object> bindParams(LogEvent event, boolean useServerFunctions)
  {
    List<Object> param = new ArrayList<>();
    Object o;
    for(Entry<Integer, Field> entry : allFields.entrySet())
    {
      Field f = entry.getValue();
      
      if (useServerFunctions) {
        if (isFieldTimestamp(f) || isFieldTimeuuid(f)) //auto generated at server
          continue;
      }
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
   * This is an asynchronous operation for batch saving as entities. A CQL is used to create a PreparedStatement once, 
   * then all row values are bound to the single PreparedStatement and executed asynchronously each, against the Session. 
   * <b>Note:</b> this method will NOT use server generated timeuuid using now() function. Will be useful
   * for inserting dates manually from application.
   * @param entities
   * @throws DataAccessException 
   * 
   */
  public void ingestEntitiesAsync(List<LogEvent> entities) throws DataAccessException
  {
    try 
    {
      Assert.notNull(entities);
      Object[] args;
      List<Object> param;
      
      final List<ResultSetFuture> futures = new ArrayList<>();
      final BatchDataAccessException dax = new BatchDataAccessException("Ingest async had errors");
      long start = 0;
      if (log.isDebugEnabled()) {
        start = System.currentTimeMillis();
        log.debug(">>> ingestEntitiesAsync: Starting ingestion batch <<<");
      }
      for(LogEvent event : entities)
      {
        param = bindParams(event, false);
        args = new Object[param.size()];
        
        futures.add(cassandraOperations.executeAsynchronously(cqlIngestEntityStmt.bind(param.toArray(args))));
        
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
      if (log.isDebugEnabled()) {
        long time = System.currentTimeMillis() - start;
        log.debug(">>> ingestEntitiesAsync: End ingestion batch <<<");
        long secs = TimeUnit.MILLISECONDS.toSeconds(time);
        log.debug("Time taken: " + secs + " secs "
            + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
      }
      if(!dax.getExceptions().isEmpty())
        throw dax;
      
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
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
    ingestAsync(entities, false);
  }
  /**
   * This is an insert operation designed for high performance writes. A CQL is used to create a PreparedStatement once, 
   * then all row values are bound to the single PreparedStatement and executed asynchronously each, against the Session. <p>
   * This method will use server generated timeuuid using now() function if useGeneratedTS is false
   * @param entities
   * @param useGeneratedTS
   * @throws DataAccessException
   */
  public void ingestAsync(List<LogEvent> entities, boolean useGeneratedTS) throws DataAccessException
  {
      
    try 
    {
      Assert.notNull(entities);
      Object[] args;
      List<Object> param;
      if (useGeneratedTS) {
        log.warn("-- Using driver generated TimeUUID instead of Cassandra now() --");
        for (LogEvent log : entities) {
          log.getId().setTimestamp(UUIDs.timeBased());
        } 
      }
      final List<ResultSetFuture> futures = new ArrayList<>();
      final BatchDataAccessException dax = new BatchDataAccessException("Ingest async had errors");
      long start = 0;
      if (log.isDebugEnabled()) {
        start = System.currentTimeMillis();
        log.debug(">>> ingestAsync:Starting ingestion batch <<<");
      }
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
      if (log.isDebugEnabled()) {
        long time = System.currentTimeMillis() - start;
        log.debug(">>> ingestAsync:End ingestion batch <<<");
        long secs = TimeUnit.MILLISECONDS.toSeconds(time);
        log.debug("Time taken: " + secs + " secs "
            + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
      }
      if(!dax.getExceptions().isEmpty())
        throw dax;
      
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }
  /**
   * 
   * This is an insert operation designed for high performance writes. A CQL is used to create a PreparedStatement once, 
   * then all row values are bound to the single PreparedStatement and executed asynchronously and <i>atomically</i> 
   * in BATCH, against the Session. <p>
   * This method will use server generated timeuuid using now() function.
   * @param entities
   * @throws DataAccessException
   * @deprecated - Batch too large exception
   */
  public void ingestBatch(List<LogEvent> entities, boolean logged) throws DataAccessException
  {
      
    try 
    {
      Assert.notNull(entities);
      Object[] args;
      List<Object> param;
      
      BatchStatement batch = new BatchStatement(logged ? Type.LOGGED : Type.UNLOGGED);
      long start = System.currentTimeMillis();
      log.info(">>> ingestAsyncBatch:Starting ingestion batch (logged) <<<");
      int i = 1;
      for(LogEvent event : entities)
      {
        
        param = bindParams(event);
        args = new Object[param.size()];
                
        batch.add(cqlIngestStmt.bind(param.toArray(args)));
        if (log.isDebugEnabled()) {
          log.debug(">>>>>>>>>> Sending ingestion params => " + param);
        }
        if((i++) % CommonHelper.CASSANDRA_MAX_BATCH_ITEMS == 0)
        {
          session.execute(batch);
          batch.clear();
        }
                
      }
      
      if(batch.size() > 0){
        session.execute(batch);
        batch.clear();
      }
      
      long time = System.currentTimeMillis() - start;
      log.info(">>> ingestAsyncBatch:End ingestion batch (logged) <<<");
      long secs = TimeUnit.MILLISECONDS.toSeconds(time);
      log.info("Time taken: "+secs+" secs "+(time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
      
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }
    
  /**
   * Inserts a single log event using a prepared statement synchronously. 
   * This method will use server generated timeuuid using now() function.
   * @param event
   * @throws DataAccessException
   */
  public void insert(LogEvent event) throws DataAccessException
  {
        
    try 
    {
      Assert.notNull(event);
      List<Object> param = bindParams(event, false);
      if (log.isDebugEnabled()) {
        log.debug(">>>>>>>>>> Sending ingestion params => " + param);
      }
      Object[] args = new Object[param.size()];
      cassandraOperations.execute(cqlIngestEntityStmt.bind(param.toArray(args)));
    } catch (Exception e) {
      throw new DataAccessException(e);
    }
  }
    
}
