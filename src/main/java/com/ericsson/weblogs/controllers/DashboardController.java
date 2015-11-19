/* ============================================================================
*
* FILE: DashboardController.java
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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ericsson.weblogs.dto.QueryRequest;
import com.ericsson.weblogs.dto.QueryResponse;
import com.ericsson.weblogs.service.ILoggingService;
import com.ericsson.weblogs.service.ServiceException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Controller
public class DashboardController {

  @Autowired
  private ILoggingService logService;
  static final String DATE_PICKER_FORMAT = "MM/dd/yyyy";
  
  @RequestMapping(value = "/logsearch")
  public @ResponseBody QueryResponse fetchLogs(@RequestParam(value = "p_appid") String appId,
      @RequestParam(value = "p_dtrange") String dateRange,
      @RequestParam(value = "p_term", required = false) String searchTerm, @RequestParam(value = "p_refresh") boolean autoRefresh, Model model) 
  {
    
    QueryResponse qr = new QueryResponse();
    QueryRequest req = new QueryRequest();
    req.setAppId(appId);
    req.setSearchTerm(searchTerm);
    //11/19/2015 - 11/19/2015
    SimpleDateFormat sdf = new SimpleDateFormat(DATE_PICKER_FORMAT);
    StringTokenizer st = new StringTokenizer(dateRange);
    try {
      req.setFromDate(sdf.parse(st.nextToken()));
      st.nextToken();
      req.setTillDate(sdf.parse(st.nextToken()));
    } catch (ParseException e) {
      log.error("Date parsing error ", e);
      qr.setError("Internal server error!");
    }
    log.debug("Got request: "+req);
    
    try 
    {
      qr = logService.fetchLogsBetweenDates(req);
    } catch (ServiceException e) {
      log.error("", e);
      qr.setError(e.getMessage());
    }
    
    return qr;
  }
}
