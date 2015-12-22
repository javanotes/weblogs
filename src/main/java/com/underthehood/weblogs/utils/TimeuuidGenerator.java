/* ============================================================================
*
* FILE: UUID1Generator.java
*
* MODULE DESCRIPTION:
* See class description
*
* Copyright (C) copied from Cassandra source file
*
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*
* ============================================================================
*/
package com.underthehood.weblogs.utils;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import lombok.extern.slf4j.Slf4j;
/**
 * This class is adapted from <a href="https://github.com/apache/cassandra/blob/cassandra-2.1/src/java/org/apache/cassandra/utils/UUIDGen.java">Cassandra source </a>
 * 
 * 
 */
@Slf4j
public final class TimeuuidGenerator {

//A grand day! millis at 00:00:00.000 15 Oct 1582.
  private static final long START_EPOCH = -12219292800000L;
  private static final long clockSeqAndNode = makeClockSeqAndNode();

  /*
   * The min and max possible lsb for a UUID.
   * Note that his is not 0 and all 1's because Cassandra TimeUUIDType
   * compares the lsb parts as a signed byte array comparison. So the min
   * value is 8 times -128 and the max is 8 times +127.
   *
   * Note that we ignore the uuid variant (namely, MIN_CLOCK_SEQ_AND_NODE
   * have variant 2 as it should, but MAX_CLOCK_SEQ_AND_NODE have variant 0).
   * I don't think that has any practical consequence and is more robust in
   * case someone provides a UUID with a broken variant.
   */
  private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
  private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

  // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
  private static final TimeuuidGenerator instance = new TimeuuidGenerator();

  private long lastNanos;

  private TimeuuidGenerator()
  {
      // make sure someone didn't whack the clockSeqAndNode by changing the order of instantiation.
      if (clockSeqAndNode == 0) throw new RuntimeException("singleton instantiation is misplaced.");
  }

  /**
   * Creates a type 1 UUID (time-based UUID).
   *
   * @return a UUID instance
   */
  public static UUID getTimeUUID()
  {
      return new UUID(instance.createTimeSafe(System.currentTimeMillis()), clockSeqAndNode);
  }

  /**
   * Creates a type 1 UUID (time-based UUID) with the timestamp of @param when, in milliseconds.
   *
   * @return a UUID instance
   */
  static UUID getTimeUUID(long when)
  {
    return new UUID(instance.createTimeSafe(when), clockSeqAndNode);
  }

  public static UUID getTimeUUIDFromMicros(long whenInMicros)
  {
      long whenInMillis = whenInMicros / 1000;
      long nanos = (whenInMicros - (whenInMillis * 1000)) * 10;
      return getTimeUUID(whenInMillis, nanos);
  }

  public static UUID getTimeUUID(long when, long nanos)
  {
      return new UUID(createTime(fromUnixTimestamp(when, nanos)), clockSeqAndNode);
  }

  
  public static UUID getTimeUUID(long when, long nanos, long clockSeqAndNode)
  {
      return new UUID(createTime(fromUnixTimestamp(when, nanos)), clockSeqAndNode);
  }

  /** creates a type 1 uuid from raw bytes. */
  public static UUID getUUID(ByteBuffer raw)
  {
      return new UUID(raw.getLong(raw.position()), raw.getLong(raw.position() + 8));
  }

  /** decomposes a uuid into raw bytes. */
  public static byte[] decompose(UUID uuid)
  {
      long most = uuid.getMostSignificantBits();
      long least = uuid.getLeastSignificantBits();
      byte[] b = new byte[16];
      for (int i = 0; i < 8; i++)
      {
          b[i] = (byte)(most >>> ((7-i) * 8));
          b[8+i] = (byte)(least >>> ((7-i) * 8));
      }
      return b;
  }

  /**
   * Returns a 16 byte representation of a type 1 UUID (a time-based UUID),
   * based on the current system time.
   *
   * @return a type 1 UUID represented as a byte[]
   */
  public static byte[] getTimeUUIDBytes()
  {
      return createTimeUUIDBytes(instance.createTimeSafe(System.currentTimeMillis()));
  }

  /**
   * Returns the smaller possible type 1 UUID having the provided timestamp.
   *
   * <b>Warning:</b> this method should only be used for querying as this
   * doesn't at all guarantee the uniqueness of the resulting UUID.
   */
  public static UUID minTimeUUID(long timestamp)
  {
      return new UUID(createTime(fromUnixTimestamp(timestamp)), MIN_CLOCK_SEQ_AND_NODE);
  }

  /**
   * Returns the biggest possible type 1 UUID having the provided timestamp.
   *
   * <b>Warning:</b> this method should only be used for querying as this
   * doesn't at all guarantee the uniqueness of the resulting UUID.
   */
  public static UUID maxTimeUUID(long timestamp)
  {
      // unix timestamp are milliseconds precision, uuid timestamp are 100's
      // nanoseconds precision. If we ask for the biggest uuid have unix
      // timestamp 1ms, then we should not extend 100's nanoseconds
      // precision by taking 10000, but rather 19999.
      long uuidTstamp = fromUnixTimestamp(timestamp + 1) - 1;
      return new UUID(createTime(uuidTstamp), MAX_CLOCK_SEQ_AND_NODE);
  }

  /**
   * @param uuid
   * @return milliseconds since Unix epoch
   */
  public static long unixTimestamp(UUID uuid)
  {
      return (uuid.timestamp() / 10000) + START_EPOCH;
  }

  /**
   * @param uuid
   * @return microseconds since Unix epoch
   */
  public static long microsTimestamp(UUID uuid)
  {
      return (uuid.timestamp() / 10) + START_EPOCH * 1000;
  }

  /**
   * @param timestamp milliseconds since Unix epoch
   * @return
   */
  private static long fromUnixTimestamp(long timestamp) {
      return fromUnixTimestamp(timestamp, 0L);
  }

  private static long fromUnixTimestamp(long timestamp, long nanos)
  {
      return ((timestamp - START_EPOCH) * 10000) + nanos;
  }

  /**
   * Converts a milliseconds-since-epoch timestamp into the 16 byte representation
   * of a type 1 UUID (a time-based UUID).
   *
   * <p><i><b>Deprecated:</b> This method goes again the principle of a time
   * UUID and should not be used. For queries based on timestamp, minTimeUUID() and
   * maxTimeUUID() can be used but this method has questionable usefulness. This is
   * only kept because CQL2 uses it (see TimeUUID.fromStringCQL2) and we
   * don't want to break compatibility.</i></p>
   *
   * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
   * invocations using identical timestamps will result in identical UUIDs.</i></p>
   *
   * @param timeMillis
   * @return a type 1 UUID represented as a byte[]
   */
  public static byte[] getTimeUUIDBytes(long timeMillis)
  {
      return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis));
  }

  /**
   * Converts a 100-nanoseconds precision timestamp into the 16 byte representation
   * of a type 1 UUID (a time-based UUID).
   *
   * To specify a 100-nanoseconds precision timestamp, one should provide a milliseconds timestamp and
   * a number 0 <= n < 10000 such that n*100 is the number of nanoseconds within that millisecond.
   *
   * <p><i><b>Warning:</b> This method is not guaranteed to return unique UUIDs; Multiple
   * invocations using identical timestamps will result in identical UUIDs.</i></p>
   *
   * @return a type 1 UUID represented as a byte[]
   */
  public static byte[] getTimeUUIDBytes(long timeMillis, int nanos)
  {
      if (nanos >= 10000)
          throw new IllegalArgumentException();
      return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis, nanos));
  }

  private static byte[] createTimeUUIDBytes(long msb)
  {
      long lsb = clockSeqAndNode;
      byte[] uuidBytes = new byte[16];

      for (int i = 0; i < 8; i++)
          uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));

      for (int i = 8; i < 16; i++)
          uuidBytes[i] = (byte) (lsb >>> 8 * (7 - i));

      return uuidBytes;
  }

  /**
   * Returns a milliseconds-since-epoch value for a type-1 UUID.
   *
   * @param uuid a type-1 (time-based) UUID
   * @return the number of milliseconds since the unix epoch
   * @throws IllegalArgumentException if the UUID is not version 1
   */
  public static long getAdjustedTimestamp(UUID uuid)
  {
      if (uuid.version() != 1)
          throw new IllegalArgumentException("incompatible with uuid version: "+uuid.version());
      return (uuid.timestamp() / 10000) + START_EPOCH;
  }

  private static long makeClockSeqAndNode()
  {
      long clock = new Random(System.currentTimeMillis()).nextLong();

      long lsb = 0;
      lsb |= 0x8000000000000000L;                 // variant (2 bits)
      lsb |= (clock & 0x0000000000003FFFL) << 48; // clock sequence (14 bits)
      lsb |= makeNode(null);                          // 6 bytes
      return lsb;
  }

  
  // needs to return two different values for the same when.
  // we can generate at most 10k UUIDs per ms.
  private synchronized long createTimeSafe(long millis)
  {
      long nanosSince = (millis - START_EPOCH) * 10000;
      if (nanosSince > lastNanos)
        lastNanos = nanosSince;
      else
        nanosSince = ++lastNanos;
      return createTime(nanosSince);
      
  }

  /** @param when time in milliseconds */
  private long createTimeUnsafe(long when)
  {
      return createTimeUnsafe(when, 0);
  }

  private long createTimeUnsafe(long when, long nanos)
  {
      long nanosSince = ((when - START_EPOCH) * 10000) + nanos;
      return createTime(nanosSince);
  }

  private static long createTime(long nanosSince)
  {
      long msb = 0L;
      msb |= (0x00000000ffffffffL & nanosSince) << 32;
      msb |= (0x0000ffff00000000L & nanosSince) >>> 16;
      msb |= (0xffff000000000000L & nanosSince) >>> 48;
      msb |= 0x0000000000001000L; // sets the version to 1.
      return msb;
  }

  private static InetAddress getActualIPv4HostAddress() throws SocketException {

    Enumeration<NetworkInterface> nets = NetworkInterface
        .getNetworkInterfaces();
    for (NetworkInterface netint : Collections.list(nets)) {
      if (netint.isUp() && !netint.isLoopback()
          && netint.getHardwareAddress() != null) {
        Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
        for (InetAddress inetAddress : Collections.list(inetAddresses)) {
          if (inetAddress.getAddress().length == 4) {
            // this is an IPv4 address
            return inetAddress;
          }

        }
      }

    }
    return null;

  }

  /**
   * Using MAC address, process id, and some env props 
   * @param addr
   * @return
   */
  private static long makeNode(InetAddress addr) {
    byte[] mac = null;
    if (addr != null) {
      try {
        mac = NetworkInterface.getByInetAddress(addr).getHardwareAddress();

      } catch (SocketException e) {
      }
    }
    if (mac == null) {
      try 
      {
        mac = NetworkInterface.getByInetAddress(getActualIPv4HostAddress())
            .getHardwareAddress();
        
        Hasher hash = Hashing.md5().newHasher();
        hash.putBytes(mac);
        
        String procId = ManagementFactory.getRuntimeMXBean().getName()
            .split("@")[0];
        hash.putBytes(procId.getBytes(StandardCharsets.UTF_8));
        
        Properties props = System.getProperties();
        hash.putBytes(props.getProperty("os.arch").getBytes(StandardCharsets.UTF_8));
        hash.putBytes(props.getProperty("os.name").getBytes(StandardCharsets.UTF_8));
        hash.putBytes(props.getProperty("os.version").getBytes(StandardCharsets.UTF_8));
        hash.putBytes(props.getProperty("java.vendor").getBytes(StandardCharsets.UTF_8));
        hash.putBytes(props.getProperty("java.version").getBytes(StandardCharsets.UTF_8));
        
        return hash.hash().asLong();
               
      } catch (SocketException e) {
        log.warn("Unable to get a valid network interface!");
        log.debug("", e);
      }
    }
    return UUID.randomUUID().getLeastSignificantBits();
  }

  public static long makeUuidv1Epoch() {
    // UUID v1 timestamp must be in 100-nanoseconds interval since 00:00:00.000
    // 15 Oct 1582.
    Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
    c.set(Calendar.YEAR, 1582);
    c.set(Calendar.MONTH, Calendar.OCTOBER);
    c.set(Calendar.DAY_OF_MONTH, 15);
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTimeInMillis();
  }

}
