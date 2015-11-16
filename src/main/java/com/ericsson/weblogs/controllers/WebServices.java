/* ============================================================================
*
* FILE: WebServices.java
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
package com.ericsson.weblogs.controllers;

import java.lang.reflect.Method;
import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.util.StringUtils;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.validation.Validator;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.ericsson.weblogs.dto.LogIngestionStatus;
import com.ericsson.weblogs.dto.LogRequest;
import com.ericsson.weblogs.dto.LogRequests;
import com.ericsson.weblogs.dto.LogResponse;
import com.ericsson.weblogs.service.ILoggingService;
import com.ericsson.weblogs.service.ServiceException;

import lombok.extern.slf4j.Slf4j;

@Slf4j@RestController()
@RequestMapping("/services")
public class WebServices {

  public static final String RESP_MSG_INV_JSON_FORMAT = "Unrecognized JSON format.";
  public static final String RESP_MSG_SUCCESS = "Logged successfully.";
  public static final String RESP_MSG_VALIDATION_ERR = "Mandatory fields missing: ";
  public static final String RESP_MSG_INT_SERV_ERR = "Internal server error.";
  
  @RequestMapping(method = {RequestMethod.GET})
  public String showServices()
  {
    StringBuilder s = new StringBuilder("+ - - - - - - - - - - - - - - - +");
    s.append("<p>");
    s.append("RESTful service end points: ").append("<p>");
    //public methods
    for(Method m : getClass().getMethods())
    {
      if(m.isAnnotationPresent(RequestMapping.class))
      {
        RequestMapping rm = m.getAnnotation(RequestMapping.class);
        
        String path = Arrays.toString(rm.value());
        if(!"[]".equals(path))
          s.append("&nbsp;&nbsp;&nbsp;&nbsp;").append(path).append("&nbsp;")
              .append(StringUtils.arrayToCommaDelimitedString(rm.method()))
              .append("<p>");
      }
    }
    s.append("+ - - - - - - - - - - - - - - - +");
    return s.toString();
  }
  @ExceptionHandler(HttpMessageNotReadableException.class)
  private @ResponseBody LogResponse handleConversionException(HttpMessageNotReadableException ex, HttpServletRequest req)
  {
    log.error("Remote host ["+req.getRemoteHost()+"] sent unrecognized JSON format: "+ex.getMostSpecificCause().getMessage());
    log.debug("--- Stacktrace ---", ex);
    return new LogResponse(LogIngestionStatus.REJECTED, RESP_MSG_INV_JSON_FORMAT);
  }
      
  @ExceptionHandler(Exception.class)
  @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "")
  private @ResponseBody LogResponse handleException(Exception ex, HttpServletRequest req)
  {
    log.error("Unhandled exception: "+ex.getMessage(), ex);
    return new LogResponse(LogIngestionStatus.UNAVAILABLE, RESP_MSG_INT_SERV_ERR);
  }
  @Autowired
  private ILoggingService logService;
  
  @RequestMapping(method = {RequestMethod.POST}, value = "/ingest")
  public LogResponse ingestRequest(@RequestBody@Valid LogRequest request, BindingResult errors, HttpServletRequest req)
  {
    if(errors.hasFieldErrors())
    {
      StringBuilder s = new StringBuilder(RESP_MSG_VALIDATION_ERR);
      for(FieldError fe : errors.getFieldErrors())
      {
        s.append("[").append(fe.getField()).append("]" );
      }
      log.error("Remote host ["+req.getRemoteHost()+"] had validation errors in request: "+s.toString());
      return new LogResponse(LogIngestionStatus.REJECTED, s.toString());
    }
    log.debug("------->>>>>>> Got request: "+request);
    try {
      logService.ingestLoggingRequest(request);
      return new LogResponse(LogIngestionStatus.SUCCESS, RESP_MSG_SUCCESS);
    } catch (ServiceException e) {
      log.error("Service exception ", e);
      return new LogResponse(LogIngestionStatus.FAILURE, e.getMessage());
    }
  }
  
  @Autowired
  private Validator validator;
  
  @RequestMapping(method = {RequestMethod.POST}, value = "/ingestbatch")
  public LogResponse ingestRequests(@RequestBody@Valid LogRequests request, BindingResult errors, HttpServletRequest req)
  {
    if(errors.hasFieldErrors())
    {
      StringBuilder s = new StringBuilder(RESP_MSG_VALIDATION_ERR);
      for(FieldError fe : errors.getFieldErrors())
      {
        s.append("[").append(fe.getField()).append("]" );
      }
      log.error("Remote host ["+req.getRemoteHost()+"] had validation errors in request: "+s.toString());
      return new LogResponse(LogIngestionStatus.REJECTED, s.toString());
    }
    for(LogRequest r : request.getBatch())
    {
      validator.validate(r, errors);
    }
    if(errors.hasFieldErrors())
    {
      StringBuilder s = new StringBuilder(RESP_MSG_VALIDATION_ERR);
      for(FieldError fe : errors.getFieldErrors())
      {
        s.append("[").append(fe.getField()).append("]" );
      }
      log.error("Remote host ["+req.getRemoteHost()+"] had validation errors in request: "+s.toString());
      return new LogResponse(LogIngestionStatus.REJECTED, s.toString());
    }
    log.debug("------->>>>>>> Got request: "+request);
    try 
    {
      logService.ingestLoggingRequests(request.getBatch());
      return new LogResponse(LogIngestionStatus.SUCCESS, RESP_MSG_SUCCESS);
    } catch (ServiceException e) {
      log.error("Service exception ", e);
      return new LogResponse(LogIngestionStatus.FAILURE, e.getMessage());
    }
  }
}
