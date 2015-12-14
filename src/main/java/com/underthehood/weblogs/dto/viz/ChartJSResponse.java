/* ============================================================================
*
* FILE: TrendChartResponse.java
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
package com.underthehood.weblogs.dto.viz;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class ChartJSResponse {

  private List<String> labels = new ArrayList<>();
  private List<ChartJSDataset> datasets = new ArrayList<>();
  private String error;
}
