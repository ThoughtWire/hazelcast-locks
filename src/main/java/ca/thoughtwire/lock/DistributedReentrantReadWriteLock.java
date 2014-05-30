package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.DistributedAtomicLong;
import ca.thoughtwire.concurrent.DistributedDataStructureFactory;
import net.jcip.annotations.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * <p>
 * Class to implement a strongly reentrant distributed Reader-Writer Lock.
 * Readers can always acquire the lock so long as there is no writer waiting.
 * Writers must wait until all active readers are finished.
 * </p>
 *
 * <b>This implementation guarantees that writers will not starve, but
 * readers may.</b>
 *
 * @author vanessa.williams
 */
@ThreadSafe
public class DistributedReentrantReadWriteLock implements ReadWriteLock {

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
    protected DistributedReentrantReadWriteLock(DistributedDataStructureFactory grid, String lockName) {

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

    /**
     * @return true if the current thread holds the lock
     */
    public boolean isHeldByCurrentThread()
    {
        return (lockImpl.numberOfThreads != 0);
    }

    /**
     * @return true if anyone has the write lock.
     */
    public boolean isWriteLocked()
    {
        return (lockImpl.writeLockedBy.get() != LockImpl.NONE);
    }

    /**
     * @return the name of this lock.
     */
    public String getLockName() { return lockName; }

    /**
     * Useful only for testing, debugging
     * @return the number of local holds on the lock
     */
    protected int getHoldCount()
    {
        return LockImpl.holds.get().count;
    }

    /**
     * Useful only for testing, debugging
     * @return the number of local holds on the read lock
     */
    protected int getReadHoldCount()
    {
        return ((DistributedReentrantReadWriteLock.ReadLock)readLock()).getHoldCount();
    }

    /**
     * Useful only for testing, debugging
     * @return the number of local holds on the write lock
     */
    protected int getWriteHoldCount()
    {
        return ((DistributedReentrantReadWriteLock.WriteLock)writeLock()).getHoldCount();
    }

    /**
     * Useful only for testing, debugging
     * @return the local threads waiting for a lock
     */
    protected Collection<Thread> getQueuedThreads()
    {
        return LockImpl.queuedThreads;
    }

    /**
     * Useful only for testing, debugging
     * @return true if the given thread is waiting for a lock.
     */
    protected boolean hasQueuedThread(Thread t)
    {
        return LockImpl.queuedThreads.contains(t);
    }

    static class LockImpl {

        LockImpl(DistributedDataStructureFactory grid, String lockName)
        {
            this.monitor = grid.getLock(lockName + "_reentrant");
            this.lockAvailable = grid.getCondition(monitor, lockName + "_reentrant_availableCondition");
            this.writeCount = grid.getAtomicLong(lockName + "_reentrant_writers");
            this.writeLockedBy = grid.getAtomicLong(lockName + "_reentrant_writeLockedBy");
        }

        /**
         * Acquire a write lock.
         *
         * @throws InterruptedException if the current thread is interrupted
         */
        void acquireExclusive() throws InterruptedException
        {
            monitor.lockInterruptibly();
            try {
                final long tid = Thread.currentThread().getId();
                if (writeLockedBy.get() == tid)
                {
                    holds.get().count++;
                }
                else
                {
                    writeCount.incrementAndGet();
                    while (numberOfThreads > 0)
                    {
                        lockAvailable.await();
                    }
                    writeCount.decrementAndGet();
                    holds.get().count = 1;
                    writeLockedBy.set(tid);
                    numberOfThreads++;
                }
            } finally {
                monitor.unlock();
            }

        }

        /**
         * Try and acquire an exclusive lock if it can be done in the time allowed.
         *
         * @param l timeout amount
         * @param timeUnit timeout units
         * @return true if lock acquired; false o/w
         * @throws InterruptedException
         */
        boolean tryAcquireExclusive(long l, TimeUnit timeUnit) throws InterruptedException
        {
            if (l <= 0) return false;

            DistributedLockUtils.ElapsedTimer timer = new DistributedLockUtils.ElapsedTimer(timeUnit.toMillis(l));
            if (!monitor.tryLock(timer.remainingMillis(), TimeUnit.MILLISECONDS)) return false;
            try {
                final long tid = Thread.currentThread().getId();
                if (writeLockedBy.get() == tid)
                {
                    holds.get().count++;
                }
                else
                {
                    writeCount.incrementAndGet();
                    while (numberOfThreads > 0)
                    {
                        if (!lockAvailable.await(timer.remainingMillis(), TimeUnit.MILLISECONDS)) {
                            writeCount.decrementAndGet();
                            return false;
                        }
                    }
                    writeCount.decrementAndGet();
                    holds.get().count = 1;
                    writeLockedBy.set(tid);
                    numberOfThreads++;
                }
            } finally {
                monitor.unlock();
            }
            return true;
        }

        /**
         * Acquire a read lock.
         *
         * @throws InterruptedException if the current thread is interrupted
         */
        void acquireShared() throws InterruptedException
        {
            monitor.lockInterruptibly();
            try {
                long threadHolds = holds.get().count;
                if (threadHolds > 0)
                {
                    holds.get().count++;
                }
                else
                {
                    // the checking of these variables is guarded by the monitor which all methods acquire
                    while (!(writeCount.get() == 0 && writeLockedBy.get() == NONE))
                    {
                        lockAvailable.await();
                    }
                    holds.get().count = 1;
                    numberOfThreads++;
                }
            } finally {
                monitor.unlock();
            }

        }

        /**
         * Acquire a shared lock only if it can be done in the given time.
         * @return true if the exclusive lock was acquired; false o/w
         * @throws InterruptedException if the thread is interrupted
         */
        boolean tryAcquireShared(long l, TimeUnit timeUnit) throws InterruptedException
        {
            if (l <= 0) return false;

            DistributedLockUtils.ElapsedTimer timer = new DistributedLockUtils.ElapsedTimer(timeUnit.toMillis(l));

            if (!monitor.tryLock(timer.remainingMillis(), TimeUnit.MILLISECONDS)) return false;
            try {
                long threadHolds = holds.get().count;
                if (threadHolds > 0)
                {
                    holds.get().count++;
                }
                else
                {
                    // the checking of these variables is guarded by the monitor which all methods acquire
                    while (!(writeCount.get() == 0 && writeLockedBy.get() == NONE))
                    {
                        if (!lockAvailable.await(timer.remainingMillis(), TimeUnit.MILLISECONDS))
                        {
                            return false;
                        }
                    }
                    holds.get().count = 1;
                    numberOfThreads++;
                }
            } finally {
                monitor.unlock();
            }
            return true;
        }

        /**
         * Release a read or write lock.
         *
         * @throws IllegalMonitorStateException if the current thread holds no locks
         */
        void release()
        {
            monitor.lock();
            try {
                holds.get().tryDecrement();
                if (holds.get().count == 0)
                {
                    numberOfThreads--;
                    writeLockedBy.set(NONE);
                    lockAvailable.signalAll();
                }
            } finally {
                monitor.unlock();
            }
        }

        /**
        * Per-thread lock counter to prevent unlocking by non-owners.
        */
        static final ThreadLocal<HoldCounter> holds = new ThreadLocal<HoldCounter>() {
            @Override
            protected HoldCounter initialValue() { return new HoldCounter(); }
        };

        final Lock monitor;
        final Condition lockAvailable;
        final DistributedAtomicLong writeCount, writeLockedBy;
        final static Collection<Thread> queuedThreads = new ArrayList<Thread>();
        int numberOfThreads = 0;

        static final long NONE = 0;
        static final long NOT_NONE = 1;
    }

    /**
     * Implementation of the reader lock.
     *
     * {@link java.util.concurrent.locks.Lock#tryLock()},
     * {@link java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)} and
     * {@link java.util.concurrent.locks.Lock#newCondition()} are not supported.
     */
    public static class ReadLock implements Lock
    {
        public ReadLock(DistributedReentrantReadWriteLock readWriteLock)
        {
            this.lockImpl = readWriteLock.lockImpl;
        }

        @Override
        public void lock() {
            try {
                LockImpl.queuedThreads.add(Thread.currentThread());
                lockImpl.acquireShared();
                readHolds.get().count++;
                LockImpl.queuedThreads.remove(Thread.currentThread());
            } catch (InterruptedException e) {
                // restore interrupt rather than swallowing or rethrowing InterruptedException
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException
        {
            throw new UnsupportedOperationException("Interruption of thread while waiting for lock is not supported.");
        }

        @Override
        public void unlock() {
            lockImpl.release();
            readHolds.get().count--;
        }

        @Override
        public boolean tryLock() {
            throw new UnsupportedOperationException(
                    "Untimed tryLock not supported; use tryLock(long l, TimeUnit timeUnit) instead");
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            LockImpl.queuedThreads.add(Thread.currentThread());
            boolean result = lockImpl.tryAcquireShared(l, timeUnit);
            LockImpl.queuedThreads.remove(Thread.currentThread());
            if (result) readHolds.get().count++;
            return result;
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("Operation not supported.");
        }

        public int getHoldCount()
        {
            return readHolds.get().count;
        }

        private final LockImpl lockImpl;

        /**
        * Per-thread read lock counter
        */
        static final ThreadLocal<HoldCounter> readHolds = new ThreadLocal<HoldCounter>() {
            @Override
            protected HoldCounter initialValue() { return new HoldCounter(); }
        };
    }

    /**
     * Implementation of the writer lock.
     *
     * {@link java.util.concurrent.locks.Lock#tryLock()},
     * {@link java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)} and
     * {@link java.util.concurrent.locks.Lock#newCondition()} are not supported.
     */
    public static class WriteLock implements Lock
    {
        public WriteLock(DistributedReentrantReadWriteLock readWriteLock)
        {
            this.lockImpl = readWriteLock.lockImpl;
        }

        @Override
        public void lock() {
            try {
                LockImpl.queuedThreads.add(Thread.currentThread());
                lockImpl.acquireExclusive();
                writeHolds.get().count++;
                LockImpl.queuedThreads.remove(Thread.currentThread());
            } catch (InterruptedException e) {
                // restore interrupt rather than swallowing or rethrowing InterruptedException
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException("Interruption of thread while waiting for lock is not supported.");
        }

        @Override
        public void unlock()
        {
            lockImpl.release();
            writeHolds.get().count--;
        }

        @Override
        public boolean tryLock() {
            throw new UnsupportedOperationException(
                    "Untimed tryLock not supported; use tryLock(long l, TimeUnit timeUnit) instead");
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            LockImpl.queuedThreads.add(Thread.currentThread());
            boolean result = lockImpl.tryAcquireExclusive(l, timeUnit);
            if (result) writeHolds.get().count++;
            LockImpl.queuedThreads.remove(Thread.currentThread());
            return result;
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("Operation not supported.");
        }

        public int getHoldCount()
        {
            return writeHolds.get().count;
        }

        public boolean isHeldByCurrentThread()
        {
            return lockImpl.writeLockedBy.get() == Thread.currentThread().getId();
        }

        private final LockImpl lockImpl;

        /**
        * Per-thread write lock counter
        */
        static final ThreadLocal<HoldCounter> writeHolds = new ThreadLocal<HoldCounter>() {
            @Override
            protected HoldCounter initialValue() { return new HoldCounter(); }
        };
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
    }

    private final String lockName;
    private final ReadLock readerLock;
    private final WriteLock writerLock;
    final LockImpl lockImpl;

}
