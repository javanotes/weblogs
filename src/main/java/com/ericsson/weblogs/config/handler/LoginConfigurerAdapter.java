/* ============================================================================
*
* FILE: LoginConfigurerAdapter.java
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
package com.ericsson.weblogs.config.handler;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.annotation.web.servlet.configuration.EnableWebMvcSecurity;
@Configuration
@EnableWebMvcSecurity
public class LoginConfigurerAdapter extends WebSecurityConfigurerAdapter {

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    
    http.csrf().disable();
    http.authorizeRequests().antMatchers("/webjars/**", "/signin", "/about", "/services/**").permitAll();
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
