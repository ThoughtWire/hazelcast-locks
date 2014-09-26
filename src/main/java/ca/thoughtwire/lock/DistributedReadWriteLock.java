package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.DistributedAtomicLong;
import ca.thoughtwire.concurrent.DistributedDataStructureFactory;
import ca.thoughtwire.concurrent.DistributedSemaphore;
import net.jcip.annotations.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static ca.thoughtwire.lock.DistributedLockUtils.ElapsedTimer;
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
    protected DistributedReadWriteLock(final DistributedDataStructureFactory grid, final String lockName) {

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
     * @return the number of local holds on the lock
     */
    public int getHoldCount()
    {
        return lockImpl.holds.get().count;
    }

    /**
     * @return the name of this lock.
     */
    public String getLockName() { return lockName; }

    /* convenience method used throughout */
    private static long getCurrentThreadId() {
        return Thread.currentThread().getId();
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
    protected boolean hasQueuedThread(final Thread t)
    {
        return LockImpl.queuedThreads.contains(t);
    }

    /* convenience methods used by debugging functions */
    private static boolean addToQueuedThreads() {
        return LockImpl.queuedThreads.add(Thread.currentThread());
    }

    private static boolean removeFromQueuedThreads() {
        boolean success = LockImpl.queuedThreads.remove(Thread.currentThread());
        if (!success) throw new IllegalStateException("Unable to remove thread from queue.");
        return success;
    }

    static class LockImpl {

        LockImpl(final DistributedDataStructureFactory grid, final String lockName)
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
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

            holds.get().tryIncrement();
            final long writecounter = writeCount.incrementAndGet();
            boolean readSemaphoreAcquired = false;
            if (writecounter == 1) {
                try {
                    readSemaphore.acquire();
                    readSemaphoreAcquired = true;
                } catch (InterruptedException e) {
                    writeCount.decrementAndGet();
                    holds.get().tryDecrement();
                    throw e;
                }
            }
            try {
            	writeSemaphore.acquire();
            } catch (InterruptedException e) {
                if (readSemaphoreAcquired) readSemaphore.release();
                writeCount.decrementAndGet();
                holds.get().tryDecrement();
                throw e;
            }
            exclusiveOwner = Thread.currentThread().getId();
        }

        /**
         * Acquire an exclusive lock only if it can be done immediately.
         * @return true if the exclusive lock was acquired; false o/w
         */
        boolean tryAcquireExclusive()
        {
            holds.get().tryIncrement();
            final long writecounter = writeCount.incrementAndGet();
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
            exclusiveOwner = getCurrentThreadId();
            return true;
        }

        /**
         * Try and acquire an exclusive lock if it can be done in the time allowed.
         *
         * @param l timeout amount
         * @param timeUnit timeout units
         * @return true if lock acquired; false o/w or if given timeout <= 0
         * @throws InterruptedException
         */
        boolean tryAcquireExclusive(final long l, final TimeUnit timeUnit) throws InterruptedException {

            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

            if (l <= 0) return false;

            final ElapsedTimer timer = new ElapsedTimer(timeUnit.toMillis(l));
            holds.get().tryIncrement();
            final long writecounter = writeCount.incrementAndGet();
            boolean readSemaphoreAcquired = false;
            if (writecounter == 1) {
                try {
					if (!readSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
					{
						writeCount.decrementAndGet();
						holds.get().tryDecrement();
						return false;
					}
                    readSemaphoreAcquired = true;
                } catch (InterruptedException e) {
                    writeCount.decrementAndGet();
                    holds.get().tryDecrement();
                    throw e;
                }
            }
            try {
				if (!writeSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS)) {
					writeCount.decrementAndGet();
					holds.get().tryDecrement();
					if (readSemaphoreAcquired) readSemaphore.release();
					return false;
				}
            } catch (InterruptedException e) {
                writeCount.decrementAndGet();
                holds.get().tryDecrement();
                if (readSemaphoreAcquired) readSemaphore.release();
                throw e;
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
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

            holds.get().tryIncrement();
            try {
				mutex.acquire();
            } catch (InterruptedException e) {
                holds.get().tryDecrement();
                throw e;
            }
            try {
				readSemaphore.acquire();
            } catch (InterruptedException e) {
                holds.get().tryDecrement();
                mutex.release();
                throw e;
            }
            final long readcounter = readCount.incrementAndGet();
            if (readcounter == 1) {
                try {
                    writeSemaphore.acquire();
                } catch (InterruptedException e) {
                    readCount.decrementAndGet();
					readSemaphore.release();
                    holds.get().tryDecrement();
					mutex.release();
                    throw e;
                }
            }
            readSemaphore.release();
            mutex.release();
			sharedOwner = getCurrentThreadId();
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
            final long readcounter = readCount.incrementAndGet();
            if (readcounter == 1)
            {
                if (!writeSemaphore.tryAcquire())
                {
                    readCount.decrementAndGet();
                    readSemaphore.release();
                    holds.get().tryDecrement();
                    mutex.release();
                    return false;
                }
            }
            readSemaphore.release();
            mutex.release();
            sharedOwner = getCurrentThreadId();
            return true;
        }

        /**
         * Acquire a shared lock only if it can be done in the given time.
         * @param l timeout amount
         * @param timeUnit timeout units
         * @return true if lock acquired; false o/w or if given timeout <= 0
         * @throws InterruptedException if the thread is interrupted
         */
        boolean tryAcquireShared(final long l, final TimeUnit timeUnit) throws InterruptedException {

            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

            if (l <= 0) return false;

            final ElapsedTimer timer = new ElapsedTimer(timeUnit.toMillis(l));
            holds.get().tryIncrement();
            try {
				if (!mutex.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
				{
					holds.get().tryDecrement();
					return false;
				}
            } catch (InterruptedException e) {
                holds.get().tryDecrement();
                throw e;
            }

            try {
				if (!readSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
				{
					holds.get().tryDecrement();
					mutex.release();
					return false;
				}
            } catch (InterruptedException e) {
                holds.get().tryDecrement();
                mutex.release();
                throw e;
            }

            final long readcounter = readCount.incrementAndGet();
            if (readcounter == 1)
            {
                try {
					if (!writeSemaphore.tryAcquire(timer.remainingMillis(), TimeUnit.MILLISECONDS))
					{
						holds.get().tryDecrement();
						mutex.release();
						readCount.decrementAndGet();
						readSemaphore.release();
						return false;
                    }
                } catch (InterruptedException e) {
                    holds.get().tryDecrement();
                    mutex.release();
                    readCount.decrementAndGet();
                    readSemaphore.release();
                    throw e;
                }
            }
            readSemaphore.release();
            mutex.release();
            sharedOwner = getCurrentThreadId();
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
            if (readCount.decrementAndGet() == 0) { writeSemaphore.release(); }
            sharedOwner = NONE;
            holds.remove();
        }

        /**
         * Release a write lock.
         */
        void releaseExclusive()
        {
            holds.get().tryDecrement();
            writeSemaphore.release();
            if (writeCount.decrementAndGet() == 0) { readSemaphore.release(); }
            exclusiveOwner = NONE;
            holds.remove();
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
        /* local threads waiting on a lock; useful for debugging */
        final static Collection<Thread> queuedThreads = new ArrayList<Thread>();
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
        public ReadLock(final DistributedReadWriteLock readWriteLock)
        {
            this.lockImpl = readWriteLock.lockImpl;
        }

        @Override
        public void lock() {
            addToQueuedThreads();
            try {
                lockImpl.acquireShared();
            } catch (InterruptedException e) {
                removeFromQueuedThreads();
                // restore interrupt rather than swallowing or rethrowing InterruptedException
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException
        {
            addToQueuedThreads();
            try {
                lockImpl.acquireShared();
            } finally {
                removeFromQueuedThreads();
            }
        }

        @Override
        public void unlock() {
            lockImpl.releaseShared();
        }

        @Override
        public boolean tryLock() {
            addToQueuedThreads();
            try {
                return lockImpl.tryAcquireShared();
            } finally {
                removeFromQueuedThreads();
            }

        }

        @Override
        public boolean tryLock(final long l, final TimeUnit timeUnit) throws InterruptedException {
            addToQueuedThreads();
            try {
                return lockImpl.tryAcquireShared(l, timeUnit);
            } finally {
                removeFromQueuedThreads();
            }
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
    public static class WriteLock implements Lock
    {
        public WriteLock(final DistributedReadWriteLock readWriteLock)
        {
            this.lockImpl = readWriteLock.lockImpl;
        }

        @Override
        public void lock() {
            addToQueuedThreads();
            try {
                lockImpl.acquireExclusive();
            } catch (InterruptedException e) {
                // restore interrupt rather than swallowing or rethrowing InterruptedException
                Thread.currentThread().interrupt();
            } finally {
                removeFromQueuedThreads();
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            addToQueuedThreads();
            try {
                lockImpl.acquireExclusive();
            } finally {
                removeFromQueuedThreads();
            }
        }

        @Override
        public void unlock()
        {
            lockImpl.releaseExclusive();
        }

        @Override
        public boolean tryLock() {
            addToQueuedThreads();
            try {
                return lockImpl.tryAcquireExclusive();
            } finally {
                removeFromQueuedThreads();
            }
        }

        @Override
        public boolean tryLock(final long l, final TimeUnit timeUnit) throws InterruptedException {
            addToQueuedThreads();
            try {
                return lockImpl.tryAcquireExclusive(l, timeUnit);
            } finally {
                removeFromQueuedThreads();
            }
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
