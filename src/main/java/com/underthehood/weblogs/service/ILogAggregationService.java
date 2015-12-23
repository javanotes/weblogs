/* ============================================================================
*
* FILE: ILogAnalyticService.java
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
package com.underthehood.weblogs.service;

import java.util.Date;
import java.util.Map;

import com.underthehood.weblogs.dto.QueryRequest;
import com.underthehood.weblogs.dto.SliceQueryRequest;

public interface ILogAggregationService {

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
  /**
   * 
   * @param request
   * @return
   * @throws ServiceException
   */
  Map<String, Map<Date, Long>> countExecutionTimings(SliceQueryRequest request) throws ServiceException;
}
