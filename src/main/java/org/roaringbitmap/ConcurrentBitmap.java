/*
 * (c) King.com Ltd, Galderic Punti
 * Licensed under the Apache License, Version 2.0.
 */

package org.roaringbitmap;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A thread-safe bitmap implementation. Concurrency is achieved by using
 * ReentrantReadWriteLocks at the container level and a ConcurrentHashMap to
 * maintain the container list integrity.
 */
public class ConcurrentBitmap {

    private ConcurrentHashMap<Short, Element> highLowMap = new ConcurrentHashMap<Short, Element>();

    public final void add(int x) {
        set(x, 1);
    }

    public void remove(int x) {
        set(x, 0);
    }

    public boolean contains(int x) {

        boolean result = false;

        final short hb = Util.highbits(x);

        Element e = highLowMap.get(hb);

        if (e != null) {

            short sb = Util.lowbits(x);
            e.rwLock.readLock().lock();
            try {
                result = e.value.contains(sb);
            } finally {
                e.rwLock.readLock().unlock();
            }
        }

        return result;
    }

    protected void set(final int x, final int bitValue) {

        final short hb = Util.highbits(x);

        Element e = highLowMap.get(hb);

        if ((e == null) && (bitValue == 1)) {
            // we have to create a new container for this x
            e = new Element(hb, new ArrayContainer());
            Element existing = highLowMap.putIfAbsent(hb, e);
            if (existing != null) {
                e = existing;
            }
        }

        // Element might still be null if we are trying to remove a bit from a
        // non-existing container. We just ignore that request, otherwise:
        if (e != null) {
            short lb = Util.lowbits(x);
            e.rwLock.writeLock().lock();
            try {
                Container container = e.value;
                final Container afterUpdate;
                if (bitValue == 1) {
                    afterUpdate = container.add(lb);
                    if (afterUpdate != container) {
                        e.value = afterUpdate;
                    }
                } else {
                    afterUpdate = container.remove(lb);
                    if (container.getCardinality() == 0) {
                        highLowMap.remove(hb);
                    } else if (afterUpdate != container) {
                        e.value = afterUpdate;
                    }
                }
            } finally {
                e.rwLock.writeLock().unlock();
            }
        }
    }

    public int getCardinality() {
        int result = 0;
        for (Element e : highLowMap.values()) {
            e.rwLock.readLock().lock();
            try {
                result += e.value.getCardinality();
            } finally {
                e.rwLock.readLock().unlock();
            }
        }
        return result;
    }

    public static final class Element implements Cloneable, Comparable<Element> {
        short key;
        Container value = null;
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

        public Element(short key, Container value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int hashCode() {
            return key * 0xF0F0F0 + value.hashCode() + rwLock.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Element) {
                Element e = (Element) o;
                return (e.key == key) && e.value.equals(value) && e.rwLock.equals(rwLock);
            }
            return false;
        }

        @Override
        public Element clone() throws CloneNotSupportedException {
            Element c = (Element) super.clone();
            // c.key copied by super.clone
            c.value = this.value.clone();
            return c;
        }

        @Override
        public int compareTo(Element o) {
            return Util.toIntUnsigned(this.key) - Util.toIntUnsigned(o.key);
        }
    }

    public IntIterator getIntIterator() {
        return new ConcurrentIntIterator();
    }

    /**
     * This iterator is backed by the bitmap data. Changing the data in the
     * bitmap will change the data being iterated. This means that the data
     * retrieved is not the 'snapshot' of the bitmap at any precise moment.
     * Instead, the iterator reflects the changes being done in the bitmap as
     * you iterate over it.
     */
    private final class ConcurrentIntIterator implements IntIterator {

        public ConcurrentIntIterator() {
            nextContainer();
        }

        private ShortIterator iter;
        TreeSet<Short> sortedKeySet = new TreeSet<Short>();

        private int x;

        private Short currentKey = Short.MIN_VALUE;

        @Override
        public boolean hasNext() {
            return currentKey != null;
        }

        private void nextContainer() {

            // update the ordered list of containers
            sortedKeySet.clear();
            sortedKeySet.addAll(highLowMap.keySet());

            boolean trySearchNextKey = true;
            Short nextKey = null;
            while ((nextKey == null) && trySearchNextKey) {
                // get the next container
                nextKey = sortedKeySet.higher(currentKey);
                trySearchNextKey = false;

                if (nextKey != null) {
                    Element e = highLowMap.get(nextKey);
                    if (e != null) {
                        // we clone the container so we are be able to
                        // iterate over it without holding any lock
                        ReentrantReadWriteLock hbLock = e.rwLock;
                        hbLock.readLock().lock();
                        iter = e.value.clone().getShortIterator();
                        hbLock.readLock().unlock();
                    } else {
                        // the container was removed between checking the
                        // next key and retrieving its container. We'll look
                        // for the following one
                        nextKey = null;
                        trySearchNextKey = true;
                    }
                }
                currentKey = nextKey;
            }
        }

        @Override
        public int next() {
            // no locking needed here, as this iterator was from a cloned
            // container
            x = Util.toIntUnsigned(iter.next()) | (currentKey << 16);
            if (!iter.hasNext()) {
                nextContainer();
            }

            return x;
        }

        @Override
        public IntIterator clone() {
            throw new UnsupportedOperationException();
        }
    }

    public int getSizeInBytes() {
        int size = 8;
        for (Map.Entry<Short, Element> e : highLowMap.entrySet()) {
            final Container c = e.getValue().value;
            e.getValue().rwLock.readLock().lock();
            try {
                size += 56 + c.getSizeInBytes();
            } finally {
                e.getValue().rwLock.readLock().lock();
            }
        }
        return size;
    }

}
