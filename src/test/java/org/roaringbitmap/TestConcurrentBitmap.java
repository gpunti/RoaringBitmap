/*
 * (c) King.com Ltd, Galderic Punti
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.junit.Test;

public class TestConcurrentBitmap {

    private final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    public final static int NUM_RANGE = 100000000;
    public final static int DENSITY_PERCENTAGE = 30;

    // One third of requests will be calls to contains() method
    public final static int CONTAINS_PROB = (1 * 100) / 3;

    // The rest will be add() and remove() method calls, distributed so that we
    // maintain the desired density percentage
    public final static int ADD_PROB = (2 * DENSITY_PERCENTAGE) / 3;
    public final static int REMOVE_PROB = (2 * (100 - DENSITY_PERCENTAGE)) / 3;

    CountDownLatch doneSignal = new CountDownLatch(NUM_THREADS);
    private final Logger logger = Logger.getLogger(getClass().getName());

    @Test
    public void testSimpleAdd() {
        ConcurrentBitmap bitmap = new ConcurrentBitmap();
        bitmap.add(308582);
        assertTrue(bitmap.contains(308582));
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {

        Random r = new Random();
        final ConcurrentBitmap bitmap = new ConcurrentBitmap();

        Executor executor = Executors.newCachedThreadPool();

        int targetBitmapSize = (NUM_RANGE / 100) * DENSITY_PERCENTAGE;

        logger.info("Filling bitmap with " + targetBitmapSize + " random numbers between 0 and " + NUM_RANGE + "...");

        Set<Integer> set = new TreeSet<Integer>();
        while (set.size() < targetBitmapSize) {
            int x = r.nextInt();
            if (x < 0) {
                x = -x;
            }
            int l = x % NUM_RANGE;
            if (l < 0) {
                l *= -1;
            }
            set.add(l);

            if (set.size() % 1000000 == 0) {
                logger.info(" Current set size:" + set.size());
            }
        }
        logger.info("Constructing bitmap...");
        for (int i : set) {
            bitmap.add(i);
        }
        logger.info("Done");

        set.clear();

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.execute(new BitmapAccessThread(bitmap, doneSignal));
        }

        doneSignal.await();
    }

    private static class BitmapAccessThread implements Runnable {

        private ConcurrentBitmap bitmap;
        private CountDownLatch doneSignal;
        private final int NUM_ITERATIONS = 50000000;
        protected Logger logger = Logger.getLogger(getClass().getName());

        public BitmapAccessThread(ConcurrentBitmap bitmap, CountDownLatch doneSignal) {
            this.bitmap = bitmap;
            this.doneSignal = doneSignal;
        }

        @Override
        public void run() {
            try {
                accessBitmapRandomly(bitmap);
            } finally {
                doneSignal.countDown();
            }
        }

        protected void accessBitmapRandomly(ConcurrentBitmap dataSource) {

            Random r = new Random();
            long totalAddNs = 0;
            long totalAdd = 0;
            long totalRemoveNs = 0;
            long totalRemove = 0;
            long totalContainsNs = 0;
            long totalContains = 0;

            int i = 0;
            while (i < NUM_ITERATIONS) {

                int l = r.nextInt() % NUM_RANGE;

                if (l < 0)
                    l = -l;

                int action = r.nextInt() % 100;

                if (action < 0) {
                    action *= -1;
                }

                if (action < ADD_PROB) {
                    long before = System.nanoTime();
                    dataSource.add(l);
                    totalAddNs += (System.nanoTime() - before);
                    totalAdd++;
                } else if (action < ADD_PROB + REMOVE_PROB) {
                    long before = System.nanoTime();
                    dataSource.remove(l);
                    totalRemoveNs += (System.nanoTime() - before);
                    totalRemove++;
                } else {
                    long before = System.nanoTime();
                    dataSource.contains(l);
                    totalContainsNs += (System.nanoTime() - before);
                    totalContains++;
                }
                i++;
                if (i % (NUM_ITERATIONS / 20) == 0) {
                    logger.info("Thread:" + Thread.currentThread() + " reached " + i + " iterations");
                }
            }

            long meanAddNs = (totalAddNs) / (totalAdd);
            long meanRemoveNs = (totalRemoveNs) / (totalRemove);
            long meanContainsNs = (totalContainsNs) / (totalContains);

            logger.info("Mean add time(ns):" + meanAddNs);
            logger.info("Mean remove time(ns):" + meanRemoveNs);
            logger.info("Mean contains time(ns):" + meanContainsNs);
        }

    }
}
