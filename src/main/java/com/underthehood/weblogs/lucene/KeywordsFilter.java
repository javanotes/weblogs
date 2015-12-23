/* ============================================================================
*
* FILE: KeywordsFilter.java
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
package com.underthehood.weblogs.lucene;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;

class KeywordsFilter extends FilteringTokenFilter {

  private final TypeAttribute typeAttribute = addAttribute(TypeAttribute.class);
  
  public KeywordsFilter(TokenStream in, Set<String> keywords,
      boolean useAsWhitelist) {
    super(in);
    this.keywords = keywords;
    this.useAsWhitelist = useAsWhitelist;
  }

  private final Set<String> keywords;
  private final boolean useAsWhitelist;
  @Override
  protected boolean accept() throws IOException {
    return useAsWhitelist ? keywords.contains(typeAttribute.toString()) : !keywords.contains(typeAttribute.toString());
  }

}
