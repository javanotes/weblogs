/* ============================================================================
*
* FILE: TimeuuidComparator.java
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
package com.underthehood.weblogs.utils;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.UUID;

public class TimeuuidComparator implements Comparator<UUID> {

  @Override
  public int compare(UUID u1, UUID u2) {
    
    return compare(ByteBuffer.wrap(TimeuuidGenerator.decompose(u1)), ByteBuffer.wrap(TimeuuidGenerator.decompose(u2)));
  }
  
  private static int compare(ByteBuffer o1, ByteBuffer o2)
  {
      if (!o1.hasRemaining() || !o2.hasRemaining())
          return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

      int res = compareTimestampBytes(o1, o2);
      if (res != 0)
          return res;
      return o1.compareTo(o2);
  }
  
  private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2)
  {
      int o1Pos = o1.position();
      int o2Pos = o2.position();

      int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
      if (d != 0) return d;

      d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
      if (d != 0) return d;

      d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
      if (d != 0) return d;

      d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
      if (d != 0) return d;

      d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
      if (d != 0) return d;

      d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
      if (d != 0) return d;

      d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
      if (d != 0) return d;

      return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
  }


}
