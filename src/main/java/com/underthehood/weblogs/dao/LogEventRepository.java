/* ============================================================================
*
* FILE: LogEventRepository.java
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
import java.util.Collections;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.underthehood.weblogs.domain.LogEvent;

import lombok.extern.slf4j.Slf4j;

@Repository
@Slf4j
public class LogEventRepository extends LogEventDAO implements CassandraRepository<LogEvent> {

  @Autowired
  private CassandraOperations cassandraOperations;

  @Override
  public <S extends LogEvent> S save(S entity) {

    return cassandraOperations.insert(entity,
        new WriteOptions(ConsistencyLevel.ONE, RetryPolicy.DEFAULT));
  }

  @Override
  public <S extends LogEvent> Iterable<S> save(Iterable<S> entities) {
        
    return cassandraOperations.insert(entities,
        new WriteOptions(ConsistencyLevel.ONE, RetryPolicy.DEFAULT));
  }
    
  private void preparePKClause(final Select select, final BasicMapId id)
  {
    Where w = select.where();
    for(Entry<Integer, Field> e : pkFields.entrySet())
    {
      if(id.containsKey(e.getValue().getName()))
        w.and(QueryBuilder.eq(e.getValue().getAnnotation(PrimaryKeyColumn.class).name(), id.get(e.getValue().getName())));
    }
  }
  
  private void preparePKClause(final Delete delete, final BasicMapId id)
  {
    com.datastax.driver.core.querybuilder.Delete.Where w = delete.where();
    for(Entry<Integer, Field> e : pkFields.entrySet())
    {
      if(id.containsKey(e.getValue().getName()))
        w.and(QueryBuilder.eq(e.getValue().getAnnotation(PrimaryKeyColumn.class).name(), id.get(e.getValue().getName())));
    }
  }
  private Select prepareSelectOne(BasicMapId id) {
    
    final Select select = QueryBuilder.select().from(table);
    
    preparePKClause(select, id);
    
    return select;
  }
  
  private Delete prepareDeleteOne(BasicMapId id) {
    
    final Delete aDelete = QueryBuilder.delete().from(table);
    
    preparePKClause(aDelete, id);
    
    return aDelete;
  }

  @Override
  public LogEvent findOne(MapId id) {
    Select select = prepareSelectOne(new BasicMapId(id));
    log.debug("Prepared select query: "+select.getQueryString());
    return cassandraOperations.selectOne(select, LogEvent.class);
  }

  @Override
  public boolean exists(MapId id) {
    return findOne(id) != null;
  }

  @Override
  public Iterable<LogEvent> findAll() {
    
    final Select select = QueryBuilder.select().from(table);
    return cassandraOperations.select(select, LogEvent.class);
  }

  @Override
  public Iterable<LogEvent> findAll(Iterable<MapId> ids) {
    log.warn("<<======== [LogEventRepository.findAll] Unsupported operation! Returns empty List ==========>>");
    return Collections.emptyList();
  }

  @Override
  public long count() {
    return cassandraOperations.count(table);
  }

  @Override
  public void delete(MapId id) {
    Delete delete = prepareDeleteOne(new BasicMapId(id));
    log.debug("Prepared delete query: "+delete.getQueryString());
    cassandraOperations.executeAsynchronously(delete).getUninterruptibly();

  }

  @Override
  public void delete(LogEvent entity) {
    cassandraOperations.delete(entity, new QueryOptions(ConsistencyLevel.ONE, RetryPolicy.DEFAULT));
  }

  @Override
  public void delete(Iterable<? extends LogEvent> entities) {
    cassandraOperations.delete(entities, new QueryOptions(ConsistencyLevel.ONE, RetryPolicy.DEFAULT));
  }

  @Override
  public void deleteAll() {
    cassandraOperations.truncate(table);

  }

}
