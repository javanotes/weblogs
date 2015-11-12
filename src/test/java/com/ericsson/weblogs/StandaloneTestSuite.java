/* ============================================================================
*
* FILE: StandaloneTestSuite.java
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
package com.ericsson.weblogs;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.ericsson.weblogs.lucene.FullTextSearch;
import com.google.common.collect.Sets;

@RunWith(BlockJUnit4ClassRunner.class)
public class StandaloneTestSuite {

  @Test
  public void testLuceneFTS()
  {
    try {
      FullTextSearch fts = new FullTextSearch();
      Set<String> tokens = fts.tokenizeText("Rama is a good boy. perhaps he was down with fever. Nad also John was down with fever as well."
          + " Perhaps I am trying to make this as long as a logging requires so that the actual story doesnt get boring. "
          + " When he was doing Lorem Ipsum and the quick brown fox ran over the crazy dog. huhh!", Sets.newHashSet("down with fever"), false, false);
      System.out.println(tokens);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
      
    }
  }
}
