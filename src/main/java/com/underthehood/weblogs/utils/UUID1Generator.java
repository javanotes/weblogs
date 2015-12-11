/* ============================================================================
*
* FILE: UUID1Generator.java
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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.hash.Hashing;

import lombok.extern.slf4j.Slf4j;
/**
 * This class is adapted from Datastax UUIDs source 
 */
@Slf4j
class UUID1Generator {

  private UUID1Generator(InetAddress address) {
    clockAndNodeLSB = getClockSeqAndNode(address);
  }

  private static UUID1Generator singleton;
  private final long clockAndNodeLSB;

  public static UUID1Generator instance() {
    if (singleton == null) {
      synchronized (UUID1Generator.class) {
        if (singleton == null) {
          singleton = new UUID1Generator(null);
        }
      }
    }
    return singleton;
  }

  /*
   * Note that currently we use System.currentTimeMillis() for a base time in
   * milliseconds, and then if we are in the same milliseconds that the previous
   * generation, we increment the number of nanoseconds. However, since the
   * precision is 100-nanoseconds intervals, we can only generate 10K UUID
   * within a millisecond safely. If we detect we have already generated that
   * much UUID within a millisecond (which, while admittedly unlikely in a real
   * application, is very achievable on even modest machines), then we stall the
   * generator (busy spin) until the next millisecond as required by the RFC.
   */
  private long getCurrentTimestamp(long timestamp) {
    while (true) {
      long now = fromUnixTimestamp(timestamp);
      long last = lastTimestamp.get();
      if (now > last) {
        if (lastTimestamp.compareAndSet(last, now))
          return now;
      } else {
        long lastMillis = millisOf(last);
        // If the clock went back in time, bail out
        if (millisOf(now) < millisOf(last))
          return lastTimestamp.incrementAndGet();

        long candidate = last + 1;
        // If we've generated more than 10k uuid in that millisecond,
        // we restart the whole process until we get to the next millis.
        // Otherwise, we try use our candidate ... unless we've been
        // beaten by another thread in which case we try again.
        if (millisOf(candidate) == lastMillis
            && lastTimestamp.compareAndSet(last, candidate))
          return candidate;
      }
    }
  }

  private final AtomicLong lastTimestamp = new AtomicLong(0L);

  private static long fromUnixTimestamp(long tstamp) {
    return (tstamp - START_EPOCH) * 10000;
  }

  private static long millisOf(long timestamp) {
    return timestamp / 10000;
  }

  /**
   * From current time
   * @return
   */
  public UUID generate() {
    return generate(System.currentTimeMillis());
  }
  
  private static long makeMSB(long timestamp) {
    long msb = 0L;
    msb |= (0x00000000ffffffffL & timestamp) << 32;
    msb |= (0x0000ffff00000000L & timestamp) >>> 16;
    msb |= (0x0fff000000000000L & timestamp) >>> 48;
    msb |= 0x0000000000001000L; // sets the version to 1.
    return msb;
  }

  /**
   * 
   * @param timestamp
   * @return
   */
  public UUID generate(long timestamp) {
    final long rawTimestamp = getCurrentTimestamp(timestamp);
    return new UUID(makeMSB(rawTimestamp), clockAndNodeLSB);
  }

  // taken from Datastax UUIDs class
  // http://www.ietf.org/rfc/rfc4122.txt
  private static final long START_EPOCH = makeEpoch();
  
  // from UUIDS commons
  private long getClockSeqAndNode(InetAddress addr) {
    
    long clock = new Random(System.currentTimeMillis()).nextLong();
    long node = makeNode(addr);

    long lsb = 0;
    lsb |= (clock & 0x0000000000003FFFL) << 48;
    lsb |= 0x8000000000000000L;
    lsb |= node;
    return lsb;
    
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
  private long makeNode(InetAddress addr) {
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
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(mac);
        String procId = ManagementFactory.getRuntimeMXBean().getName()
            .split("@")[0];
        md.update(procId.getBytes(StandardCharsets.UTF_8));
        
        Properties props = System.getProperties();
        md.update(props.getProperty("os.arch").getBytes(StandardCharsets.UTF_8));
        md.update(props.getProperty("os.name").getBytes(StandardCharsets.UTF_8));
        md.update(props.getProperty("os.version").getBytes(StandardCharsets.UTF_8));
        md.update(props.getProperty("java.vendor").getBytes(StandardCharsets.UTF_8));
        md.update(props.getProperty("java.version").getBytes(StandardCharsets.UTF_8));
                
        return Hashing.md5().hashBytes(md.digest()).asLong();
      } catch (SocketException e) {
        log.warn("Unable to get a valid network interface!");
        log.debug("", e);
      } catch (NoSuchAlgorithmException e) {
        log.error("Unable to get a message digest ingest", e);
      }
    }
    return UUID.randomUUID().getLeastSignificantBits();
  }

  private static long makeEpoch() {
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