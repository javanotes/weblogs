/* ============================================================================
*
* FILE: WebServicesController.java
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
package com.underthehood.weblogs.controllers;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.security.PermitAll;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.underthehood.weblogs.dto.LogIngestionStatus;
import com.underthehood.weblogs.dto.LogRequest;
import com.underthehood.weblogs.dto.LogRequests;
import com.underthehood.weblogs.dto.LogResponse;
import com.underthehood.weblogs.dto.ServiceInfo;
import com.underthehood.weblogs.service.ILoggingService;
import com.underthehood.weblogs.service.ServiceException;
import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller()
@RequestMapping("/api")
public class IngestionController {

  public static final String RESP_MSG_INV_JSON_FORMAT = "Invalid JSON request";
  public static final String RESP_MSG_SUCCESS = "Logged successfully.";
  public static final String RESP_MSG_VALIDATION_ERR = "Mandatory fields missing: ";
  public static final String RESP_MSG_INT_SERV_ERR = "Internal server error.";

  @PermitAll
  @RequestMapping(method = { RequestMethod.GET })
  public String showServices(Model model) {

    List<ServiceInfo> infoList = new ArrayList<>();
    // public methods
    ServiceInfo info;
    for (Method m : getClass().getMethods()) {
      if (m.isAnnotationPresent(RequestMapping.class)) {
        RequestMapping rm = m.getAnnotation(RequestMapping.class);

        String path = Arrays.toString(rm.value());
        if (!"[]".equals(path)) {

          info = new ServiceInfo();
          info.setEndPoint(path);
          info.setMethod(StringUtils.arrayToCommaDelimitedString(rm.method()));

          try {
            int i = 0;
            boolean hasBody = false;
            for (Annotation[] anns : m.getParameterAnnotations()) {
              for (Annotation a : anns) {
                if (a.annotationType() == RequestBody.class) {
                  hasBody = true;
                  break;
                }
              }
              if (hasBody) {
                break;
              }
              i++;
            }
            if (hasBody) {
              ObjectWriter wr = new ObjectMapper()
                  .writerWithDefaultPrettyPrinter();
              Object o = CommonHelper.instantiateRecursively(m.getParameterTypes()[i]);
              String json = wr
                  .writeValueAsString(o);
              json = json.replaceAll("null", "\"\"");
              info.setReq(json);

              o = CommonHelper.instantiateRecursively(m.getReturnType());
              json = wr.writeValueAsString(o);
              json = json.replaceAll("null", "\"\"");
              info.setRes(json);
            }

            infoList.add(info);

          } catch (Exception e) {
            // ignored
            log.warn("While trying to introspect service method >> "+ e.getMessage());
            log.debug("--Stacktrace--", e);
          }
        }
      }
    }
    log.info(infoList.toString());
    model.addAttribute("infoList", infoList);
    return "fragments/showservices";
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  private @ResponseBody LogResponse handleConversionException(
      HttpMessageNotReadableException ex, HttpServletRequest req) {
    log.error("Remote host [" + req.getRemoteHost()
        + "] sent unrecognized JSON format: "
        + ex.getMostSpecificCause().getMessage());
    log.debug("--- Stacktrace ---", ex);
    return new LogResponse(LogIngestionStatus.REJECTED,
        RESP_MSG_INV_JSON_FORMAT);
  }

  @ExceptionHandler(Exception.class)
  @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "")
  private @ResponseBody LogResponse handleException(Exception ex,
      HttpServletRequest req) {
    log.error("Unhandled exception: " + ex.getMessage(), ex);
    return new LogResponse(LogIngestionStatus.UNAVAILABLE,
        RESP_MSG_INT_SERV_ERR);
  }

  @Autowired
  private ILoggingService logService;

  @RequestMapping(method = { RequestMethod.POST }, value = "/ingest")
  public @ResponseBody LogResponse ingestRequest(
      @RequestBody @Valid LogRequest request, BindingResult errors,
      HttpServletRequest req) {
    if (errors.hasFieldErrors()) {
      StringBuilder s = new StringBuilder(RESP_MSG_VALIDATION_ERR);
      for (FieldError fe : errors.getFieldErrors()) {
        s.append("[").append(fe.getField()).append("]");
      }
      log.error("Remote host [" + req.getRemoteHost()
          + "] had validation errors in request: " + s.toString());
      return new LogResponse(LogIngestionStatus.REJECTED, s.toString());
    }
    log.debug("------->>>>>>> Got request: " + request);
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

  @RequestMapping(method = { RequestMethod.POST }, value = "/ingestbatch")
  public @ResponseBody LogResponse ingestRequests(
      @RequestBody @Valid LogRequests request, BindingResult errors,
      HttpServletRequest req) {
    if (errors.hasFieldErrors()) {
      StringBuilder s = new StringBuilder(RESP_MSG_VALIDATION_ERR);
      for (FieldError fe : errors.getFieldErrors()) {
        s.append("[").append(fe.getField()).append("]");
      }
      log.error("Remote host [" + req.getRemoteHost()
          + "] had validation errors in request: " + s.toString());
      return new LogResponse(LogIngestionStatus.REJECTED, s.toString());
    }
    for (LogRequest r : request.getBatch()) {
      validator.validate(r, errors);
    }
    if (errors.hasFieldErrors()) {
      StringBuilder s = new StringBuilder(RESP_MSG_VALIDATION_ERR);
      for (FieldError fe : errors.getFieldErrors()) {
        s.append("[").append(fe.getField()).append("]");
      }
      log.error("Remote host [" + req.getRemoteHost()
          + "] had validation errors in request: " + s.toString());
      return new LogResponse(LogIngestionStatus.REJECTED, s.toString());
    }
    log.debug("------->>>>>>> Got request: " + request);
    try {
      logService.ingestLoggingRequests(request.getBatch());
      return new LogResponse(LogIngestionStatus.SUCCESS, RESP_MSG_SUCCESS);
    } catch (ServiceException e) {
      log.error("Service exception ", e);
      return new LogResponse(LogIngestionStatus.FAILURE, e.getMessage());
    }
  }
}
