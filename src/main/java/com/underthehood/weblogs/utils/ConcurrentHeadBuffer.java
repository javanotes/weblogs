/* ============================================================================
 *
 * FILE: ConcurrentHeadBuffer.java
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

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A bounded, sorted buffer. It can be used as a buffer for accumulating 'n'
 * size head elements. Can be accessed concurrently. Implementation uses a lock
 * free algorithm
 * 
 * @param <T>
 */
public class ConcurrentHeadBuffer<T> {

  public ConcurrentHeadBuffer(int size) {
    buffer = new AtomicReferenceArray<T>(size);
    clear();
  }

  public ConcurrentHeadBuffer(int size, Comparator<T> comparator) {
    this(size);
    this.comparator = comparator;
  }

  @SuppressWarnings("unchecked")
  private int compare(T t1, T t2)
  {
    if (comparator != null)
    {
      return comparator.compare(t1, t2);
    }
    else if (t1 instanceof Comparable)
    {
      return ((Comparable<T>) t1).compareTo(t2);
    }
    else
    {
      throw new IllegalStateException("<T> should implement Comparable or ConcurrentHeadBuffer should be passed a Comparator");
    }
  }

  /**
   * 
   * Do a compare and swap, and get the swapped element or null, if it was not
   * swapped
   * @param i
   * 
   * @param item
   * 
   * @return
   * 
   */

  private T compareAndSwap(int i, T item)
  {
    boolean set = false, greater = false;
    T t = null;
    while (!set) {
      t = buffer.get(i);

      // either i-th element was replaced with this item
      // or by some other element

      greater = compare(item, t) > 0;
      set = buffer.compareAndSet(i, t, greater ? item : t);

    }
    return greater ? t : null;

  }

  private boolean addItem(int fromOffset, T item)
  {
    for (int i = fromOffset; i < size(); i++)
    {
      if (!buffer.compareAndSet(i, null, item))
      {
        T swapped = compareAndSwap(i, item);
        if (swapped != null)
        {

          // the item has been placed. so break. but then
          // the element currently at 'i' has been swapped. so find its new
          // position, if present
          if (i + 1 < size()) {
            addItem(swapped);
          }

          return true;
        }
      }
      else {
        return true;
      }
    }

    return false;

  }

  private boolean addItem(T item)
  {
    return addItem(0, item);
  }

  private Comparator<T> comparator;

  private final AtomicReferenceArray<T> buffer;

  public int size() {
    return buffer.length();
  }

  public boolean isEmpty() {
    for (int i = 0; i < size(); i++) {
      if (buffer.get(i) != null)
        return false;
    }

    return true;
  }

  public boolean contains(Object o) {
    if (o != null) {
      for (int i = 0; i < size(); i++) {
        if (o.equals(buffer.get(i)))
          return true;
      }

    }
    return false;
  }

  public Object[] toArray() {
    Object[] o = new Object[size()];
    return toArray(o);

  }

  @SuppressWarnings("unchecked")
  public <T1> T1[] toArray(T1[] a) {
    for (int i = 0; i < size(); i++) {
      a[i] = (T1) buffer.get(i);
    }

    return a;

  }

  @SuppressWarnings("unchecked")
  public <T1> T1[] toReverseArray(T1[] a) {
    for (int i = size() - 1, j = 0; i >= 0; i--) {
      a[j++] = (T1) buffer.get(i);
    }

    return a;

  }

  public boolean add(T t) {
    return addItem(t);

  }

  public boolean remove(Object o) {

    T b;

    if(o == null)
      return false;
    
    for (int i = 0; i < size(); i++) {

      b = buffer.get(i);

      if (o.equals(b))
      {

        if(buffer.compareAndSet(i, b, null))
        {
          //shift left elements
          for (int j = i + i; j < size(); j++) {
            //check position form start. it can be possible that another higher item has been removed in the meantime
            addItem(buffer.get(j));

          }
          return true;

        }

      }

    }

    return false;

  }

  public void clear() {
    T t;
    for (int i = 0; i < size(); i++)
    {
      boolean set = false;
      do {
        t = buffer.get(i);
        set = buffer.compareAndSet(i, t, null);
      } while (!set);
    }

  }

  public T get(int index) {
    if(index < 0 || index >= size())
      throw new ArrayIndexOutOfBoundsException("Invalid index");
    return buffer.get(index);
  }
}
