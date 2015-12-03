/* ============================================================================
*
* FILE: ILoggingService.java
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
package com.ericsson.weblogs.service;

import java.util.List;
import java.util.Map;

import com.ericsson.weblogs.dto.LogRequest;
import com.ericsson.weblogs.dto.QueryRequest;
import com.ericsson.weblogs.dto.QueryResponse;

public interface ILoggingService {

  /**
   * Synchronous insert
   * @param req
   * @throws ServiceException
   */
  void ingestLoggingRequest(LogRequest req) throws ServiceException;

  
  /**
   * Batch insert asynchronously
   * @param requests
   * @throws ServiceException
   */
  void ingestLoggingRequests(List<LogRequest> requests) throws ServiceException;

 
  /**
   * @deprecated - TBD. Not all functionality implemented
   * @param request
   * @return
   * @throws ServiceException
   */
  QueryResponse fetchLogsFromDate(QueryRequest request) throws ServiceException;
  /**
   * @deprecated - TBD. Not all functionality implemented
   * @param request
   * @return
   * @throws ServiceException
   */
  QueryResponse fetchLogsTillDate(QueryRequest request) throws ServiceException;
  /**
   * @param request
   * @return
   * @throws ServiceException
   */
  QueryResponse fetchLogsBetweenDates(QueryRequest request) throws ServiceException;
  /**
   * 
   * @param request
   * @return
   * @throws ServiceException
   */
  Map<String, Long> countHourlyLogsByLevel(QueryRequest request) throws ServiceException;
  /**
   * 
   * @param request
   * @return
   * @throws ServiceException
   */
  Map<String, Long> countDailyLogsByLevel(QueryRequest request) throws ServiceException;

}