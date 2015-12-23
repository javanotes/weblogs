/* ============================================================================
*
* FILE: WebSecurityInterceptor.java
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
package com.underthehood.weblogs.config.handler;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.authentication.configurers.provisioning.InMemoryUserDetailsManagerConfigurer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.servlet.configuration.EnableWebMvcSecurity;

import lombok.Data;
@Configuration
@EnableWebMvcSecurity
public class WebSecurityInterceptor extends WebSecurityConfigurerAdapter {

  @Data
  private static class SimpleUserCredentials
  {
    private Map<String, String> credentials = new HashMap<>();
    
  }
  @Bean@ConfigurationProperties(prefix = "weblogs.user")
  public SimpleUserCredentials simpleUserCredentials()
  {
     return new SimpleUserCredentials();
  }
  @Override
  public void configure(AuthenticationManagerBuilder auth) throws Exception {
    InMemoryUserDetailsManagerConfigurer<AuthenticationManagerBuilder> authMgr = auth.inMemoryAuthentication();
    authMgr.withUser("admin").password("root123#").roles("ADMIN");//system user hard coded
    for(Entry<String, String> entry : simpleUserCredentials().getCredentials().entrySet())
    {
     authMgr.withUser(entry.getKey()).password(entry.getValue()).roles("USER");
    }
                 
                  
  }
  @Override
  protected void configure(HttpSecurity http) throws Exception {
    
    http.csrf().disable();
    http.authorizeRequests().antMatchers("/webjars/**", "/signin", "/about", "/api/**").permitAll();
    http.authorizeRequests().antMatchers("/**").hasAnyRole("USER", "ADMIN");
          
    http.formLogin()
        .passwordParameter("password").usernameParameter("username")
        .loginProcessingUrl("/authenticate")
        .defaultSuccessUrl("/welcome")
        .loginPage("/signin").permitAll()
        .and().logout()
        .invalidateHttpSession(true)
        .deleteCookies("JSESSIONID")
        .permitAll();
  }
  
  static void clearSessionCookie(HttpServletRequest request)
  {
    HttpSession session = request.getSession(false);
    Cookie[] cookies = request.getCookies();
    if(cookies != null)
    {
      for(Cookie c : cookies)
      {
        if("JSESSIONID".equals(c.getName()))
        {
          c.setMaxAge(0);
          break;
        }
      }
    }
    if(session != null)
    {
      session.invalidate();
      
    }
  }
}
