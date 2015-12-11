/* ============================================================================
*
* FILE: CyclicBarrierExample.java
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
package com.underthehood.weblogs;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

class CyclicBarrierExample {

//Runnable task for each thread
  private static class Task implements Runnable {

      private CyclicBarrier barrier;

      public Task(CyclicBarrier barrier) {
          this.barrier = barrier;
      }

      @Override
      public void run() {
          while (doWhile) {
            try {
              System.out.println(
                  Thread.currentThread().getName() + " is waiting on barrier");
              barrier.await();
              System.out.println(Thread.currentThread().getName()
                  + " has crossed the barrier");
              
            } catch (InterruptedException ex) {
              ex.printStackTrace();
            } catch (BrokenBarrierException ex) {
              ex.printStackTrace();
            } 
          }
      }
  }

  static boolean doWhile = true;
  static int count = 0;
  public static void main(String args[]) {

      //creating CyclicBarrier with 3 parties i.e. 3 Threads needs to call await()
      final CyclicBarrier cb = new CyclicBarrier(3, new Runnable(){
          @Override
          public void run(){
              if(count++ < 3){
                System.out.println("All parties are arrived at barrier, lets play");
                
              }
              else{
                System.out.println("Now we will stop");
                doWhile = false;
              }
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                
              }
          }
      });

      //starting each of thread
      Thread t1 = new Thread(new Task(cb), "Thread 1");
      Thread t2 = new Thread(new Task(cb), "Thread 2");
      Thread t3 = new Thread(new Task(cb), "Thread 3");

      t1.start();
      t2.start();
      t3.start();
   
  }


}
