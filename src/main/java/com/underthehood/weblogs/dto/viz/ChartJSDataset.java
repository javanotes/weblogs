/* ============================================================================
*
* FILE: ChartDataset.java
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
package com.underthehood.weblogs.dto.viz;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;

@Data
public class ChartJSDataset {

  List<String> data = new ArrayList<>();
  public ChartJSDataset()
  {
    
  }
  public ChartJSDataset(Collection<String> data)
  {
    this.getData().addAll(data);
  }
  private String fillColor = "rgba(220,220,220,0.2)";
  private String strokeColor = "rgba(220,220,220,1)";
  private String pointColor = "rgba(220,220,220,1)";
  private String pointStrokeColor = "#fff";
  private String pointHighlightFill = "#fff";
  private String pointHighlightStroke = "rgba(220,220,220,1)";
  //add more if needed
  
}
