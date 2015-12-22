/* ============================================================================
*
* FILE: FullTextSearch.java
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
package com.underthehood.weblogs.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.springframework.stereotype.Component;

import com.underthehood.weblogs.utils.CommonHelper;

import lombok.extern.slf4j.Slf4j;
/**
 * @deprecated - Using stratio lucene plugin
 */
@Component
@Slf4j
class FullTextSearch {

  private static Set<String> getTokens(Analyzer analyzer, String string) {
    Set<String> result = new HashSet<String>();
    try {
      TokenStream stream  = analyzer.tokenStream(null, new StringReader(string));
      stream.reset();
      while (stream.incrementToken()) {
        result.add(stream.getAttribute(CharTermAttribute.class).toString());
      }
      stream.end();
      stream.close();
    } catch (IOException e) {
      log.warn("Exception using Lucene ", e);
    }
    return result;
  }
  
  private static Set<String> permuteString(String string)
  {
    String[] words = string.split(" ");
    Set<String> permutes = new HashSet<>();
    if(words.length > 0)
    {
      for(int i=0; i<words.length; i++)
      {
        String append = "";
        for(int j=i+1; j<words.length; j++)
        {
          append += words[j] + " ";
          permutes.add(words[i] +" "+ append);
        }
      }
    }
    return permutes;
  }
  /**
   * Tokenize a text to create a reverse index lookup for searching.
   * @param text
   * @return
   */
  public Set<String> tokenizeText(String text)
  {
    return tokenizeText(text, Collections.<String> emptySet(), false, false);
  }
  /**
   * Tokenize a text to create a reverse index lookup for searching. Tokens are in lower case
   * @param text The string to tokenize
   * @param keywords Specific keywords/phrases to index
   * @param useAllTerms Whether to use all term permutations including keywords (if present)
   * @return
   */
  public Set<String> tokenizeText(String text, Set<String> keywords, boolean useAllTerms)
  {
    return tokenizeText(text, keywords, false, useAllTerms);
  }
  /**
   * Tokenize a text to create a reverse index lookup for searching. Tokens are in lower case
   * @param text The string to tokenize
   * @param keywords Specific keywords/phrases to index
   * @param asBlacklist Whether to use it as a blacklist (when we don't search if term is present)
   * @param useAllTerms Whether to use all term permutations including keywords (if present)
   * @return
   */
  public Set<String> tokenizeText(String text, Set<String> keywords, boolean asBlacklist, boolean useAllTerms)
  {
    final Set<String> tokens = new HashSet<>();
    LogTextAnalyzer az = new LogTextAnalyzer();
    az.setUseAsWhitelist(!asBlacklist);
    if(CommonHelper.isCollectionEmptyOrNull(keywords))
    {
      az.setEmitSingleTokens(true);
    }
    else
    {
      az.keywords.addAll(keywords);
      az.setEmitSingleTokens(false);
    }
    tokens.addAll(getTokens(az, text));
    
    if(useAllTerms)
      tokens.addAll(permuteString(text));
    
    return tokens;
    
  }
 
}
