/*
 * (c) King.com Ltd, Galderic Punti
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static org.junit.Assert.*;

public class TestConcurrentBitmap {

    public final static int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    public final static int NUM_ITERATIONS = 1000000;
    public final static int NUM_RANGE = 10000000;
    public final int DENSITY_PERCENTAGE = 30;

    private final Logger logger = Logger.getLogger(getClass().getName());

    @Test
    public void testSimpleAdd() {
        BitmapInterface bitmap = new ConcurrentBitmap();
        bitmap.add(308582);
        assertTrue(bitmap.contains(308582));
    }

    @Test
    public void testSimpleCheckedAdd() {
        BitmapInterface bitmap = new ConcurrentBitmap();
        assertTrue(bitmap.checkedAdd(308582));
        assertFalse(bitmap.checkedAdd(308582));
        assertTrue(bitmap.contains(308582));
    }

    @Test
    public void testSimpleCheckedRemove() {
        BitmapInterface bitmap = new ConcurrentBitmap();
        bitmap.add(308582);
        assertTrue(bitmap.contains(308582));
        assertTrue(bitmap.checkedRemove(308582));
        assertFalse(bitmap.checkedRemove(308582));
    }

    @Test
    public void testConcurrentAdd() throws InterruptedException {

        Executor executor = Executors.newCachedThreadPool();

        final Random r = new Random();
        final long[] threadSeeds = new long[NUM_THREADS];
        for (int i = 0; i < threadSeeds.length; i++) {
            threadSeeds[i] = r.nextLong();
        }

        final BitmapInterface concurrentBitmap = new ConcurrentBitmap();

        final CountDownLatch threadsReady = new CountDownLatch(NUM_THREADS);
        final CountDownLatch threadsFinished = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            final long seed = threadSeeds[i];
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    Random r = new Random(seed);
                    threadsReady.countDown();
                    try {
                        threadsReady.await();
                        logger.info("Starting thread:" + Thread.currentThread());
                        for (int i = 0; i < NUM_ITERATIONS; i++) {
                            concurrentBitmap.add(r.nextInt(NUM_RANGE));
                        }
                        threadsFinished.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        threadsFinished.await();
        assertTrue(concurrentBitmap.getCardinality() <= NUM_ITERATIONS * NUM_THREADS);

        // check that every integer added by any thread is contained in the
        // final resulting concurrentBitmap
        RoaringBitmap controlMap = new RoaringBitmap();
        for (int i = 0; i < NUM_THREADS; i++) {
            Random ri = new Random(threadSeeds[i]);
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                int v = ri.nextInt(NUM_RANGE);
                assertTrue("Every integer added in each thread must be in the final result", concurrentBitmap.contains(v));
                controlMap.add(v);
            }
        }
        assertEquals("Adding in parallel or sequentially must have the same result size", controlMap.getCardinality(), concurrentBitmap.getCardinality());

    }

    @Test
    public void testConcurrentRemove() throws InterruptedException {

        Executor executor = Executors.newCachedThreadPool();

        final Random seedsRandom = new Random();
        final long[] seeds = new long[NUM_THREADS];
        for (int i = 0; i < seeds.length; i++) {
            seeds[i] = seedsRandom.nextLong();
        }

        final BitmapInterface concurrentBitmap = new ConcurrentBitmap();

        final Random mapContentsRandom = new Random(478202868L);

        for (int i = 0; i < 50000000; i++) {
            concurrentBitmap.add(mapContentsRandom.nextInt(NUM_RANGE));
        }

        final CountDownLatch readyToStart = new CountDownLatch(NUM_THREADS);
        final CountDownLatch finished = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            final long seed = seeds[i];
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    Random r = new Random(seed);
                    readyToStart.countDown();
                    logger.info("Starting thread:" + Thread.currentThread());
                    try {
                        readyToStart.await();
                        for (int i = 0; i < NUM_ITERATIONS; i++) {
                            concurrentBitmap.remove(r.nextInt(NUM_RANGE));
                        }
                        finished.countDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        finished.await();
        RoaringBitmap removedElementsMap = new RoaringBitmap();
        for (int i = 0; i < NUM_THREADS; i++) {
            Random ri = new Random(seeds[i]);
            for (int j = 0; j < NUM_ITERATIONS; j++) {
                int v = ri.nextInt(NUM_RANGE);
                removedElementsMap.add(v);
            }
        }

        // check that the resulting concurrentBitmap has exactly the original
        // integers except those removed
        final Random controlRandom = new Random(478202868L);
        for (int i = 0; i < 50000000; i++) {
            int v = controlRandom.nextInt(NUM_RANGE);
            if (removedElementsMap.contains(v)) {
                assertFalse("This integer was removed by some thread. It can NOT be in the resulting bitmap", concurrentBitmap.contains(v));
            } else {
                assertTrue("This integer was NOT removed by any thread. It must still be in the resulting bitmap", concurrentBitmap.contains(v));
            }
        }
    }

    @Test
    public void testConcurrentAddAndRemove() throws InterruptedException {

        Random r = new Random(97494028L);
        final BitmapInterface concurrentBitmap = new ConcurrentBitmap();

        Executor executor = Executors.newCachedThreadPool();
        CountDownLatch threadsFinished = new CountDownLatch(NUM_THREADS);

        int targetBitmapSize = (NUM_RANGE / 100) * DENSITY_PERCENTAGE;

        logger.info("Filling bitmap with " + targetBitmapSize + " random numbers between 0 and " + NUM_RANGE + "...");

        Set<Integer> set = new TreeSet<Integer>();
        while (set.size() < targetBitmapSize) {
            int x = r.nextInt(NUM_RANGE);

            set.add(x);

            if (set.size() % 1000000 == 0) {
                logger.info(" Current set size:" + set.size());
            }
        }
        logger.info("Constructing bitmap...");
        for (int i : set) {
            concurrentBitmap.add(i);
        }
        logger.info("Done");

        set.clear();

        RoaringBitmap[] addedElementsByThread = new RoaringBitmap[NUM_THREADS];
        RoaringBitmap[] removedElementsByThread = new RoaringBitmap[NUM_THREADS];

        for (int i = 0; i < NUM_THREADS; i++) {
            RoaringBitmap addedElements = new RoaringBitmap();
            RoaringBitmap removedElements = new RoaringBitmap();
            addedElementsByThread[i] = addedElements;
            removedElementsByThread[i] = removedElements;
            executor.execute(new BitmapAccessThread(concurrentBitmap, threadsFinished, addedElements, removedElements));
        }

        threadsFinished.await();

        // The final result will be slightly different on each execution, as
        // add()'s and remove()'s of the same numbers might execute in different
        // sequence order. We can only look for impossible scenarios.

        RoaringBitmap totalAdded = FastAggregation.or(addedElementsByThread);
        RoaringBitmap totalRemoved = FastAggregation.or(removedElementsByThread);

        logger.info("Total added:" + totalAdded.getCardinality());
        logger.info("Total removed:" + totalRemoved.getCardinality());

        Random regenerate = new Random(97494028L);
        long totalAssertions = 0;
        for (int i = 0; i < targetBitmapSize; i++) {
            int v = regenerate.nextInt(NUM_RANGE);
            if (!totalAdded.contains(v) && totalRemoved.contains(v)) {
                // original elements not added by any thread and removed by
                // some thread, should not be in the final result for sure
                assertFalse("This integer, for sure, should not be in the resulting bitmap", concurrentBitmap.contains(v));
                totalAssertions++;
            }
            if (!totalRemoved.contains(v)) {
                // original elements not removed by any thread, should still be
                // in the final result for sure
                assertTrue("This integer, for sure, should still be in the resulting bitmap", concurrentBitmap.contains(v));
                totalAssertions++;
            }
        }

        logger.info("Assertions on the original elements in the map:" + totalAssertions);

        // any element added by some thread and not removed by any other,
        // should be in the final result for sure
        totalAssertions = 0;
        IntIterator addedIter = totalAdded.getIntIterator();
        while (addedIter.hasNext()) {
            int v = addedIter.next();
            if (!totalRemoved.contains(v)) {
                assertTrue(concurrentBitmap.contains(v));
                totalAssertions++;
            }
        }
        logger.info("Assertions on the added and not removed elements in the map:" + totalAssertions);

    }

    class BitmapAccessThread implements Runnable {

        private BitmapInterface bitmap;
        private CountDownLatch doneSignal;
        protected Logger logger = Logger.getLogger(getClass().getName());
        final RoaringBitmap addedElements;
        final RoaringBitmap removedElements;

        public BitmapAccessThread(BitmapInterface bitmap, CountDownLatch doneSignal, RoaringBitmap addedElements, RoaringBitmap removedElements) {
            this.bitmap = bitmap;
            this.doneSignal = doneSignal;
            this.addedElements = addedElements;
            this.removedElements = removedElements;
        }

        @Override
        public void run() {
            try {
                accessBitmapRandomly();
            } finally {
                doneSignal.countDown();
            }
        }

        protected void accessBitmapRandomly() {

            // 2/3 of accesses will be add() and remove() method calls,
            // distributed so that we maintain the desired density percentage.
            // The remaining 1/3 will be contains() method calls.
            final int ADD_PROB = (2 * DENSITY_PERCENTAGE) / 3;
            final int REMOVE_PROB = (2 * (100 - DENSITY_PERCENTAGE)) / 3;

            Random r = new Random();
            long totalAddNs = 0;
            long totalAdd = 0;
            long totalRemoveNs = 0;
            long totalRemove = 0;
            long totalContainsNs = 0;
            long totalContains = 0;

            int i = 0;
            while (i < NUM_ITERATIONS) {

                int l = r.nextInt(NUM_RANGE);

                int action = r.nextInt(100);

                if (action < ADD_PROB) {
                    long before = System.nanoTime();
                    bitmap.add(l);
                    addedElements.add(l);
                    totalAddNs += (System.nanoTime() - before);
                    totalAdd++;
                } else if (action < ADD_PROB + REMOVE_PROB) {
                    long before = System.nanoTime();
                    bitmap.remove(l);
                    removedElements.add(l);
                    totalRemoveNs += (System.nanoTime() - before);
                    totalRemove++;
                } else {
                    long before = System.nanoTime();
                    bitmap.contains(l);
                    totalContainsNs += (System.nanoTime() - before);
                    totalContains++;
                }
                if (i++ % (NUM_ITERATIONS / 20) == 0) {
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

    @Test
    public void testIntIterator() {
        Random r = new Random(97494028L);
        final BitmapInterface concurrentBitmap = new ConcurrentBitmap();

        logger.info("Filling bitmap with " + NUM_ITERATIONS + " random numbers between 0 and " + NUM_RANGE + "...");

        Set<Integer> set = new TreeSet<Integer>();
        while (set.size() < NUM_ITERATIONS) {
            int x = r.nextInt(NUM_RANGE);
            set.add(x);
            if (set.size() % 1000000 == 0) {
                logger.info(" Current set size:" + set.size());
            }
        }
        logger.info("Constructing bitmap...");
        for (int i : set) {
            concurrentBitmap.add(i);
        }
        logger.info("Done");

        IntIterator iter = concurrentBitmap.getIntIterator();
        
        long total = 0;
        while (iter.hasNext()) {
            assertTrue(set.contains(iter.next()));
            total++;
        }
        
        assertEquals(NUM_ITERATIONS,total);

        set.clear();
    }
}
