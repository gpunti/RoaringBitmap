package org.roaringbitmap;

public interface BitmapInterface {

    void add(int x);

    void remove(int x);

    boolean contains(int x);

    int getCardinality();

    IntIterator getIntIterator();

    int getSizeInBytes();

    IntIterator getIterator();

    boolean checkedAdd(int x);

    boolean checkedRemove(int x);

}