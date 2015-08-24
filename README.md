A concurrent version of RoaringBitmap

An attempt was made to maintain the original array of containers, but that required an external synchronization mechanism to maintain the integrity of the array while changing the size and moving the elements. When usign a ReentrantReadWriteLock to achieve that, the performance was penalized by a factor of x5. That was because every transaction had to acquire that single readlock (even when only reading) thus becoming a bottleneck.

ConcurrentHashMap has already the locking mechanism build in and optimized for concurrent reads and writes. Also the access is faster than the list, as no binary search must be performed. Of course this comes at the price of more memory usage, specifically 44 bytes overhead per entry, plus the size of the container-level ReentrantReadWriteLock ( but that lock was necessary either way ).

It was also considered checking the bit existence in the container before setting or removing it. This would avoid us having to write-lock the container without any need. This enhancement though required two bitmap accesses per transaction and showed no performance improvement at all, even when all accesses were only contains().

Iterating on a ConcurrentBitmap is quite different from iterating in a single-threaded bitmap. While iterating a ConcurrentBitmap the bitmap itself is being modified. In fact while iterating and moving from one container to the next the bitmap might have completely changed. The CncurrentBitmap iterator guarantees the following:

 - Each integer is only iterated once.
 - After having iterated over a container, we choose the nearest following container available at that very moment, to continue iteration.
 - Integers are iterated in ascending order.
 
 The final result of the iteration is not the bitmap at any 'precise moment in time', but a combination of what the bitmap has been during the iteration. This is so because iterating over a 'snapshot' of the bitmap at certain point would require making a whole copy of it being very inefficent.

As stated before, the performance on a single core is slightly improved (at the cost of more memory usage) but the real improvement is shown when using all the cores. The following shows the number of concurrent operations being held. 1/3 of operations are contains() and 2/3 are a combination of add() and remove() that keeps the desired density of 30% ( 30M integers in the 100M range ). Not surprising the maximum througput is achieved when using 8 threads on a 8 virtual-core machine ( i7 mac laptop )
