/* ============================================================================
 *
 * FILE: MvcConfiguration.java
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

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.thymeleaf.spring4.SpringTemplateEngine;
import org.thymeleaf.spring4.view.ThymeleafView;
import org.thymeleaf.spring4.view.ThymeleafViewResolver;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.templateresolver.TemplateResolver;

import com.ericsson.weblogs.config.handler.LoggingInterceptor;
import com.ericsson.weblogs.config.handler.ThymeleafLayoutInterceptor;

@Configuration
@EnableWebMvc
public class MvcConfiguration extends WebMvcConfigurerAdapter {

 
  @Autowired
  private ThymeleafLayoutInterceptor layoutInterceptor;
  @Autowired
  private LoggingInterceptor logInterceptor;

  /*@Bean(name = "multipartResolver")
  public CommonsMultipartResolver multipartResolver() {
    log.info("Loading the multipart resolver");
    CommonsMultipartResolver multipartResolver = new CommonsMultipartResolver();
    return multipartResolver;
  }*/

  @Bean
  public TemplateResolver getTemplateResolver() {
    ClassLoaderTemplateResolver resolver = BeanUtils
        .instantiate(ClassLoaderTemplateResolver.class);
    resolver.setPrefix("templates/");
    resolver.setSuffix(".html");
    resolver.setTemplateMode("HTML5");
    resolver.setOrder(1);
    return resolver;
  }

  @Bean
  public ViewResolver getTilesViewResolver() {
    
    ThymeleafViewResolver viewResolver = new ThymeleafViewResolver();
    viewResolver.setViewClass(ThymeleafView.class);
    
    viewResolver.setTemplateEngine(getTemplateEngine());
    viewResolver.setExcludedViewNames(new String[] { "webjars/*" });
    viewResolver.setOrder(Ordered.HIGHEST_PRECEDENCE);
    
    return viewResolver;
  }
  
  @Bean
  public SpringTemplateEngine getTemplateEngine() {
    SpringTemplateEngine templateEngine = BeanUtils
        .instantiate(SpringTemplateEngine.class);
    templateEngine.setTemplateResolver(getTemplateResolver());
    
    return templateEngine;
  }

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {

    if (!registry.hasMappingForPattern("/webjars/**")) {
      registry.addResourceHandler("/webjars/**").addResourceLocations(
          "classpath:/META-INF/resources/webjars/");
      
    }
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    //registry.addInterceptor(menuLoadingInterceptor);
    registry.addInterceptor(layoutInterceptor);
    registry.addInterceptor(logInterceptor);
    //registry.addInterceptor(logbackInterceptor);
  }
  
  @Bean
  public RestTemplate restSupport()
  {
    return new RestTemplate();
  }

}
