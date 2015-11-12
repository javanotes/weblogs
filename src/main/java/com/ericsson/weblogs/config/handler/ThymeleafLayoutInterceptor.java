/* ============================================================================
 *
 * FILE: ThymeleafLayoutInterceptor.java
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.ericsson.weblogs.controllers.LoginController;

//import com.ericsson.cac.dmweb.ui.Layout;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ThymeleafLayoutInterceptor extends HandlerInterceptorAdapter {

  private static final String DEFAULT_LAYOUT = "layouts/default";
  private static final String LOGIN_LAYOUT = "layouts/signin";
  
  private static final String DEFAULT_VIEW_ATTRIBUTE_NAME = "view";

  private String defaultLayout = DEFAULT_LAYOUT;
  private String viewAttributeName = DEFAULT_VIEW_ATTRIBUTE_NAME;

  public void setDefaultLayout(String defaultLayout) {
    Assert.hasLength(defaultLayout);
    this.defaultLayout = defaultLayout;
  }

  public void setViewAttributeName(String viewAttributeName) {
    Assert.hasLength(defaultLayout);
    this.viewAttributeName = viewAttributeName;
  }

  @Override
  public void postHandle(HttpServletRequest request,
      HttpServletResponse response, Object handler, ModelAndView modelAndView)
      throws Exception {
    if (modelAndView == null || !modelAndView.hasView()) {
      return;
    }
    
    String originalViewName = modelAndView.getViewName();
    if (isRedirectOrForward(originalViewName)) {
      return;
    }
    String layoutName = getLayoutName(handler, request, originalViewName);

    log.info("Layout Name : {}", layoutName);
    log.info("originalViewName : {}", originalViewName);

    modelAndView.setViewName(layoutName);
    modelAndView.addObject(this.viewAttributeName, originalViewName);
  }

  private boolean isRedirectOrForward(String viewName) {
    return viewName.startsWith("redirect:") || viewName.startsWith("forward:");
  }

  /**
   * 
   * @param handler
   * @param originalViewName
   * @return
   */
  private String getLayoutName(Object handler, HttpServletRequest request, String originalViewName) {
    HandlerMethod h = (HandlerMethod) handler;
    if(h.getBeanType() == LoginController.class && (request.getRequestURI().contains("signin") || request.getRequestURI().equals("/")))
    {
      return LOGIN_LAYOUT;
    }
    return DEFAULT_LAYOUT;
  }
}
