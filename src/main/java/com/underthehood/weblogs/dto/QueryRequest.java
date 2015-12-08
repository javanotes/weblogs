/* ============================================================================
*
* FILE: QueryRequest.java
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
package com.underthehood.weblogs.dto;

import java.util.Date;

import lombok.Data;

@Data
public class QueryRequest {
  /**
   * Page size
   */
  private int fetchSize;
  /**
   * Set true if fetching prev page
   */
  private boolean isFetchPrev;
  /**
   * Current page reference timeuuid. Last row for 'next', first row for 'prev'
   */
  private String fetchMarkUUID;
  private String appId,searchTerm;
  private Date fromDate, tillDate;
  private String level;
}
