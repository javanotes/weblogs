/* ============================================================================
 *
 * FILE: CassandraConfiguration.java
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

package com.underthehood.weblogs.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class CassandraConfiguration {

  @Value("${cassandra.contactpoints}")
  private String nodes = "localhost";
  @Value("${cassandra.port}")
  private int port = 9042;
  @Value("${cassandra.keyspace}")
  private String keyspace = "demodb";
  
  @Bean
  public CassandraClusterFactoryBean cluster() {

    CassandraClusterFactoryBean cluster = new CassandraClusterFactoryBean();
    cluster.setContactPoints(nodes);
    cluster.setPort(port);
    log.debug(">>>>>>>>>> Connecting to Cassandra host: "+nodes+"\t on Port: "+port);
    return cluster;
  }

  @Bean
  public CassandraMappingContext mappingContext() {
    return new BasicCassandraMappingContext();
  }

  @Bean
  public CassandraConverter converter() {
    return new MappingCassandraConverter(mappingContext());
  }

  
  
  @Bean
  public CassandraSessionFactoryBean session() throws Exception {

    CassandraSessionFactoryBean session = new EntityFinderSessionFactoryBean();
    session.setCluster(cluster().getObject());
    session.setKeyspaceName(keyspace);
    session.setConverter(converter());
    session.setSchemaAction(SchemaAction.CREATE);
    log.info("Using keyspace: "+keyspace+" with SchemaAction "+session.getSchemaAction());
    return session;
  }

  @Bean
  public CassandraOperations cassandraTemplate() throws Exception {
    return new CassandraTemplate(session().getObject());
  }
  
}
