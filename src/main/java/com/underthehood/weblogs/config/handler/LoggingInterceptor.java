/* ============================================================================
*
* FILE: LoggingInterceptor.java
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.core.MethodParameter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ValueConstants;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import lombok.extern.slf4j.Slf4j;
@Component
@Slf4j
public class LoggingInterceptor extends HandlerInterceptorAdapter {

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
    HandlerMethod h = (HandlerMethod) handler;
    
    if (log.isDebugEnabled()) {
      String reqStr = h.getBeanType().getSimpleName()+"::"+h.getMethod().getName();
      for (MethodParameter p : h.getMethodParameters()) {
        if (p.hasParameterAnnotation(RequestParam.class)) {
          RequestParam rp = p.getParameterAnnotation(RequestParam.class);
          if (StringUtils.hasText(rp.value())) {
            String val = StringUtils.hasLength(request.getParameter(rp.value()))
                ? request.getParameter(rp.value()) : ValueConstants.DEFAULT_NONE.equals(rp.defaultValue()) ? "" : rp.defaultValue();
            log.debug(
                reqStr+" >> Param [" + rp.value() + "] Value [" + val + "]");
          }
        }
      } 
      
    }
    
    return true;
  }
}
