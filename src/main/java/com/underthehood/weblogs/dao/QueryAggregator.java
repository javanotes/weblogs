/* ============================================================================
*
* FILE: ParallelQueryAggregator.java
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraConverterRowCallback;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.underthehood.weblogs.domain.LogEvent;
import com.underthehood.weblogs.utils.ConcurrentHeadBuffer;
import com.underthehood.weblogs.utils.TimeuuidComparator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class QueryAggregator{
  
  private CassandraConverterRowCallback<LogEvent> rowToDomainMapper;
  /**
   * A dummy class acting as a poison pill
   */
  private static class StopSignal implements ResultSetFuture
  {

    @Override
    public void addListener(Runnable listener, Executor executor) {
      
    }

    @Override
    public ResultSet get() throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public ResultSet get(long arg0, TimeUnit arg1)
        throws InterruptedException, ExecutionException, TimeoutException {
      return null;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public ResultSet getUninterruptibly() {
      return null;
    }

    @Override
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit)
        throws TimeoutException {
      return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }
    
  }
  private final BlockingQueue<ResultSetFuture> queue = new LinkedBlockingQueue<>();
  private ExecutorService threads;
  private ConcurrentHeadBuffer<LogEvent> events;
  /**
   * Submit a {@link ResultSetFuture future} for collating the resultset
   * @param future
   */
  public void submit(ResultSetFuture future)
  {
    if(!started)
      throw new IllegalStateException("ParallelQueryAggregator not started");
    if(stopping)
      throw new IllegalStateException("ParallelQueryAggregator is stopping. No more request can be added");
    queue.offer(future);
  }
  private volatile boolean started, stopping;
  /**
   * 
   * @param nThreads no of threads
   * @param bufferSize head buffer size
   */
  public QueryAggregator(int nThreads, int bufferSize)
  {
    noOfThreads = nThreads;
    init(bufferSize);
  }
 /**
  * 
  * @param nThreads no of threads
  * @param bufferSize head buffer size
  */
  private void init(int bufferSize)
  {
    rowToDomainMapper = new CassandraConverterRowCallback<>(new MappingCassandraConverter(), LogEvent.class);
    events = new ConcurrentHeadBuffer<>(bufferSize, new Comparator<LogEvent>() {

      
      private final TimeuuidComparator uuidc = new TimeuuidComparator();
           
      @Override
      public int compare(LogEvent o1, LogEvent o2) {
          return uuidc.compare(o1.getId().getTimestamp(), o2.getId().getTimestamp());
      }
    });
    
    threads = Executors.newFixedThreadPool(noOfThreads);
    
  }
  private int noOfThreads;
  /**
   * Starts the aggregator consumer threads
   */
  public void start()
  {
    futures = new ArrayList<>(noOfThreads);
    for (int i = 0; i < noOfThreads; i++) 
    {
      Future<?> f = threads.submit(new Runnable() {
        
        @Override
        public void run() {
          doRun();
        }
      });
      futures.add(f);
    }
    threads.shutdown();
    started = true;
  }
  private List<Future<?>> futures;
  /**
   * 
   * @param duration
   * @param unit
   * @return
   */
  public List<LogEvent> awaitResultUninterruptibly(long duration, TimeUnit unit)
  {
    queue.offer(new StopSignal());
        
    try 
    {
      boolean b = threads.awaitTermination(30, TimeUnit.SECONDS);
      if(!b)
      {
        log.warn("** Parallel aggregation threads did not stop in 30 secs. Results may be surprising **");
      }
    } catch (InterruptedException e) {
      log.debug("", e);
      Thread.currentThread().interrupt();
    }
    
    List<LogEvent> list = new ArrayList<>();
    LogEvent e;
    for (int i = 0; i < events.size(); i++) {
      if((e = events.get(i)) != null)
      {
        list.add(e);
      }
    }
    return list;
  }
  /**
   * 
   * @return
   */
  public List<LogEvent> awaitResultUninterruptibly()
  {
    return awaitResultUninterruptibly(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }
  
  private void doRun() 
  {
    ResultSetFuture f;
    while(!stopping)
    {
      try 
      {
        f = queue.take();
        if(f instanceof StopSignal)
        {
          stopping = true;   
          for(Future<?> fu : futures)
          {
            fu.cancel(true);
          }
          break;
        }
          
        try 
        {
          
          final ResultSet rs = f.getUninterruptibly();
          LogEvent l;
          while(!rs.isExhausted())
          {
            l = rowToDomainMapper.doWith(rs.one());
            events.add(l);
          }
          
        } catch (Exception e) {
          log.error("Query execution failure ", e);
        }
        
      } catch (InterruptedException e) {
        log.debug("This is an interrupt to end blocking wait", e);
      }
    }
    
  }

}
