package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.DistributedAtomicLong;
import ca.thoughtwire.concurrent.DistributedDataStructureFactory;
import ca.thoughtwire.concurrent.DistributedSemaphore;
import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * <p>
 * Class to implement a distributed Reader-Writer Lock.
 * Readers can always acquire the lock so long as there is no writer waiting.
 * Writers must wait until all active readers are finished.
 * </p>
 *
 * <b>This implementation guarantees that writers will not starve, but
 * readers may. It is not reentrant.</b>
 *
 * @author vanessa.williams
 */
@ThreadSafe
public class DistributedReadWriteLock implements ReadWriteLock {

    /**
     * Constructor for a distributed multiple-reader, single-writer
     * lock which guarantees that writers do not starve.
     *
     * Not intended to be used directly. Use {@link DistributedLockFactory#getReadWriteLock(String)}
     * instead, to take care of providing a concrete implementation of DistributedDataStructureFactory.
     *
     * @param grid factory for distributed semaphores and atomic longs
     * @param lockName name of the lock
     * @throws NullPointerException if either argument is null
     */
    protected DistributedReadWriteLock(DistributedDataStructureFactory grid, String lockName) {

        if (grid == null || lockName == null) throw new NullPointerException("All arguments required.");

        this.lockImpl = new LockImpl(grid, lockName);
        this.lockName = lockName;
        this.readerLock = new ReadLock(this);
        this.writerLock = new WriteLock(this);
    }

    @Override
    public Lock readLock() { return readerLock; }

    @Override
    public Lock writeLock() { return writerLock; }

    public int getHoldCount()
    {
        return lockImpl.holds.get().count;
    }

    /**
     * @return the name of this lock.
     */
    public String getLockName() { return lockName; }

    class LockImpl {

        LockImpl(DistributedDataStructureFactory grid, String lockName)
        {
            this.mutex = grid.getSemaphore(lockName + "_mutex", 1);
            this.readSemaphore = grid.getSemaphore(lockName + "_read", 1);
            this.writeSemaphore = grid.getSemaphore(lockName + "_write", 1);
            this.readCount = grid.getAtomicLong(lockName + "_readers");
            this.writeCount = grid.getAtomicLong(lockName + "_writers");
        }

        /**
         * Acquire a write lock.
         *
         * @throws InterruptedException if the current thread is interrupted
         * @throws IllegalThreadStateException if the thread already holds a lock
         */
        void acquireExclusive() throws InterruptedException
        {
            holds.get().tryIncrement();
            long writecounter = writeCount.incrementAndGet();
            if (writecounter == 1) { readSemaphore.acquire(); }
            writeSemaphore.acquire();
            exclusiveOwner = Thread.currentThread().getId();
        }

        /**
         * Acquire an exclusive lock only if it can be done immediately.
         * @return true if the exclusive lock was acquired; false o/w
         */
        boolean tryAcquireExclusive()
        {
            holds.get().tryIncrement();
            long writecounter = writeCount.incrementAndGet();
            if (writecounter == 1) {
                if (!readSemaphore.tryAcquire())
                {
                    writeCount.decrementAndGet();
                    holds.get().tryDecrement();
                    return false;
                }
            }
            if (!writeSemaphore.tryAcquire()) {
                writeCount.decrementAndGet();
                holds.get().tryDecrement();
                readSemaphore.release();
                return false;
            }
            exclusiveOwner = Thread.currentThread().getId();
            return true;
        }

        /**
         * Try and acquire an exclusive lock if it can be done in the time allowed.
         *
         * @param l timeout amount
         * @param timeUnit timeout units
         * @return true if lock acquired; false o/w
         * @throws InterruptedException
         */
        boolean tryAcquireExclusive(long l, TimeUnit timeUnit) throws InterruptedException {
            if (l <= 0) return false;

            DistributedLockUtils.ElapsedTimer timer = new DistributedLockUtils.ElapsedTimer(timeUnit.toMillis(l));
            holds.get().tryIncrement();
            long writecounter = writeCount.incrementAndGet();
            if (writecounter == 1) {
                if (!readSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
                {
                    writeCount.decrementAndGet();
                    holds.get().tryDecrement();
                    return false;
                }
            }
            if (!writeSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS)) {
                writeCount.decrementAndGet();
                holds.get().tryDecrement();
                readSemaphore.release();
                return false;
            }
            exclusiveOwner = Thread.currentThread().getId();
            return true;
        }

        /**
         * Acquire a read lock.
         *
         * @throws InterruptedException if the current thread is interrupted
         */
        void acquireShared() throws InterruptedException
        {
            holds.get().tryIncrement();
            mutex.acquire();
            readSemaphore.acquire();
            long readcounter = readCount.incrementAndGet();
            if (readcounter == 1) { writeSemaphore.acquire(); }
            readSemaphore.release();
            mutex.release();
            sharedOwner = Thread.currentThread().getId();
        }

        /**
         * Acquire a shared lock only if it can be done immediately.
         * @return true if the exclusive lock was acquired; false o/w
         */
        boolean tryAcquireShared() {
            holds.get().tryIncrement();
            if (!mutex.tryAcquire())
            {
                holds.get().tryDecrement();
                return false;
            }
            if (!readSemaphore.tryAcquire())
            {
                holds.get().tryDecrement();
                mutex.release();
                return false;
            }
            long readcounter = readCount.incrementAndGet();
            if (readcounter == 1)
            {
                if (!writeSemaphore.tryAcquire())
                {
                    holds.get().tryDecrement();
                    mutex.release();
                    readCount.decrementAndGet();
                    readSemaphore.release();
                    return false;
                }
            }
            readSemaphore.release();
            mutex.release();
            sharedOwner = Thread.currentThread().getId();
            return true;
        }

        /**
         * Acquire a shared lock only if it can be done in the given time.
         * @return true if the exclusive lock was acquired; false o/w
         * @throws InterruptedException if the thread is interrupted
         */
        boolean tryAcquireShared(long l, TimeUnit timeUnit) throws InterruptedException {

            if (l <= 0) return false;

            DistributedLockUtils.ElapsedTimer timer = new DistributedLockUtils.ElapsedTimer(timeUnit.toMillis(l));
            holds.get().tryIncrement();
            if (!mutex.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
            {
                holds.get().tryDecrement();
                return false;
            }
            if (!readSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
            {
                holds.get().tryDecrement();
                mutex.release();
                return false;
            }
            long readcounter = readCount.incrementAndGet();
            if (readcounter == 1)
            {
                if (!writeSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
                {
                    holds.get().tryDecrement();
                    mutex.release();
                    readCount.decrementAndGet();
                    readSemaphore.release();
                    return false;
                }
            }
            readSemaphore.release();
            mutex.release();
            sharedOwner = Thread.currentThread().getId();
            return true;
        }

        /**
         * Release a read lock.
         *
         * @throws IllegalMonitorStateException if the current thread holds no locks
         */
        void releaseShared()
        {
            holds.get().tryDecrement();
            long readcounter = readCount.decrementAndGet();
            if (readcounter == 0) { writeSemaphore.release(); }
            sharedOwner = NONE;
         }

        /**
         * Release a write lock.
         */
        void releaseExclusive()
        {
            holds.get().tryDecrement();
            writeSemaphore.release();
            long writecounter = writeCount.decrementAndGet();
            if (writecounter == 0) { readSemaphore.release(); }
            exclusiveOwner = NONE;
        }

        /**
        * Per-thread lock counter to prevent reentrance and unlocking by non-owners.
        */
        final ThreadLocal<HoldCounter> holds = new ThreadLocal<HoldCounter>() {
            @Override
            protected HoldCounter initialValue() { return new HoldCounter(); }
        };

        /*
         * Semaphores and counters for a "writers-preference" implementation
         * of a reader-writer lock.
         */
        final DistributedSemaphore mutex, readSemaphore, writeSemaphore;
        final DistributedAtomicLong readCount, writeCount;

        static final long NONE = 0;
        long exclusiveOwner = NONE;
        long sharedOwner = NONE;
    }

    /**
     * Implementation of the reader lock.
     *
     * {@link java.util.concurrent.locks.Lock#tryLock()},
     * {@link java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)} and
     * {@link java.util.concurrent.locks.Lock#newCondition()} are not supported.
     */
    public class ReadLock implements Lock
    {
        public ReadLock(DistributedReadWriteLock readWriteLock)
        {
            this.lockImpl = readWriteLock.lockImpl;
        }

        @Override
        public void lock() {
            try {
                lockImpl.acquireShared();
            } catch (InterruptedException e) {
                // restore interrupt rather than swallowing or rethrowing InterruptedException
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException
        {
            lockImpl.acquireShared();
        }

        @Override
        public void unlock() {
            lockImpl.releaseShared();
        }

        @Override
        public boolean tryLock() {
            return lockImpl.tryAcquireShared();
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            return lockImpl.tryAcquireShared(l, timeUnit);
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("Operation not supported.");
        }

        private final LockImpl lockImpl;
    }

    /**
     * Implementation of the writer lock.
     *
     * {@link java.util.concurrent.locks.Lock#tryLock()},
     * {@link java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)} and
     * {@link java.util.concurrent.locks.Lock#newCondition()} are not supported.
     */
    public class WriteLock implements Lock
    {
        public WriteLock(DistributedReadWriteLock readWriteLock)
        {
            this.lockImpl = readWriteLock.lockImpl;
        }

        @Override
        public void lock() {
            try {
                lockImpl.acquireExclusive();
            } catch (InterruptedException e) {
                // restore interrupt rather than swallowing or rethrowing InterruptedException
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            lockImpl.acquireExclusive();
        }

        @Override
        public void unlock()
        {
            lockImpl.releaseExclusive();
        }

        @Override
        public boolean tryLock() {
            return lockImpl.tryAcquireExclusive();
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            return lockImpl.tryAcquireExclusive(l, timeUnit);
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("Operation not supported.");
        }

        private final LockImpl lockImpl;
    }

    /*
     * Counter for per-thread lock hold counts. Maintained as a ThreadLocal.
     */
    static final class HoldCounter
    {
        int count;
        final long tid = Thread.currentThread().getId();

        /*
         * Convenience method to detect illegal attempts to release locks that are not held
         * by the calling thread.
         */
        public int tryDecrement()
        {
            count--;
            if (count < 0)
            {
                count = 0;
                throw new IllegalMonitorStateException("Current thread is not the holder of the lock.");
            }
            return count;
        }

        public int tryIncrement()
        {
            count++;
            if (count > 1)
            {
                count = 1;
                throw new IllegalThreadStateException("Current thread already holds the lock.");
            }
            return count;
        }
    }

    private final String lockName;
    private final ReadLock readerLock;
    private final WriteLock writerLock;
    final LockImpl lockImpl;
}
