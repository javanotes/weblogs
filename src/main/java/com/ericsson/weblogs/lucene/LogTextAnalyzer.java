/* ============================================================================
*
* FILE: LogTextAnalyzer.java
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
package com.ericsson.weblogs.lucene;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;

import lombok.Setter;

class LogTextAnalyzer extends Analyzer {

  final Set<String> keywords = new HashSet<>();
  private final static Set<String> characters = new HashSet<>();
  static{
    for(char i=32; i<=126; i++)
    {
      characters.add(Character.toString(i));
    }
  }
  private boolean useAsWhitelist = true;
  public boolean isUseAsWhitelist() {
    return useAsWhitelist;
  }

  public void setUseAsWhitelist(boolean useAsWhitelist) {
    this.useAsWhitelist = useAsWhitelist;
  }

  LogTextAnalyzer addKeyword(String key) {
    keywords.add(key);
    return this;
  }
  
  private final CharArraySet stopWords = CharArraySet.copy(characters);

  @Setter
  private boolean emitSingleTokens = true;
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new ClassicTokenizer();
    
    stopWords.addAll(StopAnalyzer.ENGLISH_STOP_WORDS_SET);
    
    TokenFilter filter = new ClassicFilter(source);
    filter = new LowerCaseFilter(filter);
    filter = new AutoPhrasingTokenFilter(filter);
    ((AutoPhrasingTokenFilter) filter).setPhraseMap(keywords);
    ((AutoPhrasingTokenFilter) filter).setEmitSingleTokens(emitSingleTokens);
    filter = new StopFilter(filter, stopWords);
        
    return new TokenStreamComponents(source, filter);
   
  }

}
