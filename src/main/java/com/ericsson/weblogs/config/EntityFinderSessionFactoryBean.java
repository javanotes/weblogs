/* ============================================================================
*
* FILE: EntityFinderSessionFactoryBean.java
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
package com.ericsson.weblogs.config;

import static org.springframework.cassandra.core.cql.CqlIdentifier.cqlId;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.cassandra.core.keyspace.CreateIndexSpecification;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.data.cassandra.config.CassandraSessionFactoryBean;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.Indexed;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.util.ReflectionUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class EntityFinderSessionFactoryBean extends CassandraSessionFactoryBean
{
  public EntityFinderSessionFactoryBean() {
    super();
  }
  
  /**
   * Find classes with @Table annotation
   * @return
   */
  private static Collection<Class<?>> findEntityClasses()
  {
    ClassPathScanningCandidateComponentProvider provider= new ClassPathScanningCandidateComponentProvider(false);
    provider.addIncludeFilter(new TypeFilter() {
      
      @Override
      public boolean match(MetadataReader metadataReader,
          MetadataReaderFactory metadataReaderFactory) throws IOException {
        AnnotationMetadata aMeta = metadataReader.getAnnotationMetadata();
        return aMeta.hasAnnotation(Table.class.getName());
      }
    });
    Set<BeanDefinition> beans =  provider.findCandidateComponents("com/ericsson/weblogs/domain");
    
    return Collections2.transform(beans, new Function<BeanDefinition, Class<?>>() {

      @Override
      public Class<?> apply(BeanDefinition input) {
        try 
        {
          Class<?> clazz = Class.forName(input.getBeanClassName());
          return clazz;
        } catch (ClassNotFoundException e) {
          log.error("Exception while scanning for entity classes ", e);
        }
        return null;
      }
    });
    
  }
  
  private void autoUpdateDDL()
  {
    Collection<Class<?>> entities = findEntityClasses();
    for (Class<?> entity : entities) {
      
      //create tables if not exists
      final String table = AnnotationUtils.findAnnotation(entity, Table.class).value();
      admin.createTable(true, CqlIdentifier.cqlId(table), entity, null); 
      
      //created indexes if not exists
      ReflectionUtils.doWithFields(entity, new ReflectionUtils.FieldCallback() {
        
        @Override
        public void doWith(Field field)
            throws IllegalArgumentException, IllegalAccessException {
          String colName;
          try {
            colName = field.getAnnotation(Column.class).value();
          } catch (Exception e) {
            throw new IllegalArgumentException("@Column annotation not present on an @Indexed column field "+field);
          }
          CreateIndexSpecification idx = new CreateIndexSpecification()
          .tableName(table)
          .ifNotExists(true)
          .columnName(colName)
          .name("idx_"+colName);//throw exception if anything fails
          admin.execute(idx);
          
        }
      }, new ReflectionUtils.FieldFilter() {
        
        @Override
        public boolean matches(Field field) {
          return field.isAnnotationPresent(Indexed.class);
        }
      });
    }
  }
  @Override
  protected void createTables(boolean dropTables, boolean dropUnused) {

    //this section is from default Spring data setup
    Metadata md = session.getCluster().getMetadata();
    KeyspaceMetadata kmd = md.getKeyspace(keyspaceName);

    // TODO: fix this with KeyspaceIdentifier
    if (kmd == null) { // try lower-cased keyspace name
      kmd = md.getKeyspace(keyspaceName.toLowerCase());
    }

    if (kmd == null) {
      throw new IllegalStateException(String.format("keyspace [%s] does not exist", keyspaceName));
    }

    for (TableMetadata table : kmd.getTables()) {
      if (dropTables) {
        if (dropUnused || mappingContext.usesTable(table)) {
          admin.dropTable(cqlId(table.getName()));
        }
      }
    }
    
    //this section is overridden by finding entity classes from domain package
    autoUpdateDDL();
            
  }
}