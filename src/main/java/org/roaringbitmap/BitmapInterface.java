package org.roaringbitmap;

public interface BitmapInterface {

    /**
     * 
     * @param x element to be added
     * @return if the operation modified the bitmap cardinality
     */
    boolean add(int x);

    /**
     * 
     * @param x element to be removed
     * @return if the operation modified the bitmap cardinality
     */
    boolean remove(int x);

    boolean contains(int x);

    int getCardinality();

    IntIterator getIntIterator();

    int getSizeInBytes();

    IntIterator getIterator();

}