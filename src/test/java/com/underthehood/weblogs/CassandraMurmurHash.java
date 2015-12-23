/* ============================================================================
*
* FILE: CassandraMurmurHash.java
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
package com.underthehood.weblogs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CassandraMurmurHash {

  public long getToken(String key)
  {
      return getToken(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)));
  }
  public long getToken(ByteBuffer key)
  {
      return getToken(key, getHash(key));
  }

  private long getToken(ByteBuffer key, long[] hash)
  {
      if (key.remaining() == 0)
          return Long.MIN_VALUE;

      return normalize(hash[0]);
  }

  private long[] getHash(ByteBuffer key)
  {
      long[] hash = new long[2];
      MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, hash);
      return hash;
  }


  private long normalize(long v)
  {
      // We exclude the MINIMUM value; see getToken()
      return v == Long.MIN_VALUE ? Long.MAX_VALUE : v;
  }
}
