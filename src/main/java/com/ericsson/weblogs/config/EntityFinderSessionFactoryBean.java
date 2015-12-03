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
import org.springframework.util.StringUtils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.ericsson.weblogs.domain.annot.CustomIndexField;
import com.ericsson.weblogs.domain.annot.CustomIndexOption;
import com.ericsson.weblogs.domain.annot.CustomIndexed;
import com.ericsson.weblogs.domain.annot.LuceneIndex;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.stratio.cassandra.lucene.builder.Builder;
import com.stratio.cassandra.lucene.builder.index.Index;
import com.stratio.cassandra.lucene.builder.index.schema.Schema;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class EntityFinderSessionFactoryBean extends CassandraSessionFactoryBean
{
  public EntityFinderSessionFactoryBean() {
    super();
  }
  @Getter
  private String logTextColumn;
  private void createCustomIndex(Class<?> entity, String table)
  {

    //create custom indexes
    CustomIndexed cidx = entity.getAnnotation(CustomIndexed.class);
    String colName = null;
            
    for(Field f : entity.getDeclaredFields())
    {
      if(f.isAnnotationPresent(LuceneIndex.class)){
        try {
          colName = f.getAnnotation(Column.class).value();
          break;
        } catch (Exception e) {
          throw new IllegalArgumentException("@Column annotation not present on an @LuceneIndex column field "+f);
        }
      }
    }
    if(!StringUtils.hasText(colName))
      throw new IllegalArgumentException("@Column name not present on an @LuceneIndex column field ");
      
    if(!StringUtils.hasText(cidx.className()))
      throw new IllegalArgumentException("@CustomIndexed className is empty ");
    
    Index c_idx = Builder.index(table, colName).name("c_idx_" + colName);
    c_idx.defaultAnalyzer("english").refreshSeconds(1);
    
    String idxDDL;
    CustomIndexOption opt = cidx.option();
    Schema schema = Builder.schema();
    for(CustomIndexField cf : opt.schema().fields())
    {
      
      switch(cf.type())
      {
      case "text":
        schema.mapper(cf.field(), Builder.textMapper().analyzer(cf.analyzer()).sorted(cf.sorted()));
        logTextColumn = cf.field();//we know we are indexing only this column
        break;
      case "string":
        schema.mapper(cf.field(), Builder.stringMapper().sorted(cf.sorted()));
        break;
      case "date":
        schema.mapper(cf.field(), Builder.dateMapper().pattern(cf.pattern()).sorted(cf.sorted()));
        break;
      case "double":
        schema.mapper(cf.field(), Builder.doubleMapper().sorted(cf.sorted()));
        break;
      case "integer":
        schema.mapper(cf.field(), Builder.integerMapper().sorted(cf.sorted()));
        break;
      case "long":
        schema.mapper(cf.field(), Builder.longMapper().sorted(cf.sorted()));
        break;
      case "uuid":
        schema.mapper(cf.field(), Builder.uuidMapper().sorted(cf.sorted()));
        break;
        default: break;
      }
      
    }
    
    c_idx.schema(schema);
    
    idxDDL = c_idx.build().replaceFirst("CREATE CUSTOM INDEX", "CREATE CUSTOM INDEX IF NOT EXISTS");
    
    log.debug("Custom index DDL: "+idxDDL);
    
    admin.execute(idxDDL);
  
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
              .tableName(table).ifNotExists(true).columnName(colName)
              .name("idx_" + colName);//throw exception if anything fails
          admin.execute(idx);
          
        }
      }, new ReflectionUtils.FieldFilter() {
        
        @Override
        public boolean matches(Field field) {
          return field.isAnnotationPresent(Indexed.class);
        }
      });
      
      //custom index
      if(entity.isAnnotationPresent(CustomIndexed.class))
      {
        createCustomIndex(entity, table);
      }
      
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