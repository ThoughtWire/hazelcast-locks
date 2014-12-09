package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.*;
import junit.framework.AssertionFailedError;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.*;

/**
 * A set of classes and methods useful for testing and mocking.
 * Putting them here instead of in the test tree allows easier
 * usage by others.
 *
 * @author vanessa.williams
 */
public class DistributedLockUtils {

    DistributedLockUtils() {}

    /**
     * Subclass of {@link DistributedReentrantReadWriteLock} to expose some
     * internals for testing.
     */
    public class PublicDistributedReentrantReadWriteLock extends DistributedReentrantReadWriteLock
    {
        public PublicDistributedReentrantReadWriteLock(DistributedDataStructureFactory grid, String lockName) {
            super(grid, lockName);
        }

        /**
         * Protected method exposed for testing
         * @return the number of holds this thread has on the lock (read + write)
         */
        public int getHoldCount()
        {
            return super.getHoldCount();
        }

        /**
         * Protected method exposed for testing
         * @return the number of holds this thread has on the read lock
         */
        public int getReadHoldCount()
        {
            return super.getReadHoldCount();
        }

        /**
         * Protected method exposed for testing
         * @return the number of holds this thread has on the write lock
         */
        public int getWriteHoldCount()
        {
            return super.getWriteHoldCount();
        }

        /**
         * Protected method exposed for testing
         * @return the threads in this process waiting for a lock
         */
        public Collection<Thread> getQueuedThreads()
        {
            return super.getQueuedThreads();
        }

        /**
         * Protected method exposed for testing
         * @return true if the given thread is waiting for a lock
         */
        public boolean hasQueuedThread(Thread t)
        {
            return super.hasQueuedThread(t);
        }
    }

    public class PublicDistributedLockService extends DistributedLockService
    {
        public PublicDistributedLockService(HazelcastDataStructureFactory dataStructureFactory)
        {
            super(dataStructureFactory);
        }

        @Override
        public DistributedReentrantReadWriteLock getReentrantReadWriteLock(String lockName) {
            if (THREAD_LOCKS.containsKey(lockName)) {
                return THREAD_LOCKS.get(lockName);
            } else {
                PublicDistributedReentrantReadWriteLock lock =
                        new PublicDistributedReentrantReadWriteLock(distributedDataStructureFactory, lockName);
                THREAD_LOCKS.put(lockName, lock);
                return lock;
            }
        }

        final Map<String, PublicDistributedReentrantReadWriteLock> THREAD_LOCKS =
                new HashMap<String, PublicDistributedReentrantReadWriteLock>();
    }

    public static class LocalAtomicLong implements DistributedAtomicLong
    {
        public LocalAtomicLong(String name)
        {
            this.delegate = new AtomicLong();
            this.name = name;
        }

        @Override
        public String getName() { return name; }

        @Override
        public boolean compareAndSet(long l, long l2) { return delegate.compareAndSet(l, l2); }

        @Override
        public long get() { return delegate.get(); }

        @Override
        public void set(long l) { delegate.set(l); }

        @Override
        public long getAndSet(long l) { return delegate.getAndSet(l); }

        @Override
        public long getAndIncrement() { return delegate.getAndIncrement(); }

        public long getAndDecrement() { return delegate.getAndDecrement(); }

        @Override
        public long getAndAdd(long l) { return delegate.getAndAdd(l); }

        @Override
        public long decrementAndGet() { return delegate.decrementAndGet(); }

        @Override
        public long incrementAndGet() { return delegate.incrementAndGet(); }

        @Override
        public long addAndGet(long l) { return delegate.addAndGet(l); }

        private final AtomicLong delegate;
        private final String name;
    }

    /**
     * Extends Semaphore to provide public access to protected methods.
     */
    public static class PublicSemaphore extends Semaphore
    {
        public PublicSemaphore(int permits, boolean fair)
        {
            super(permits, fair);
        }

        public Collection<Thread> getQueuedThreads()
        {
            return super.getQueuedThreads();
        }
    }

    public static class LocalSemaphore implements DistributedSemaphore
    {
        public LocalSemaphore(String name, int permits)
        {
            this.delegate = new PublicSemaphore(permits, true);
            this.name = name;
        }

        @Override
        public String getName() { return name; }

        @Override
        public void acquire() throws InterruptedException {
            delegate.acquire();
        }

        @Override
        public void release() {
            delegate.release();
        }

        @Override
        public int availablePermits() {
            return delegate.availablePermits();
        }

        @Override
        public boolean tryAcquire() {
            return delegate.tryAcquire();
        }

        @Override
        public boolean tryAcquire(long l, TimeUnit timeUnit) throws InterruptedException {
            return delegate.tryAcquire(l, timeUnit);
        }

        private final String name;
        private final PublicSemaphore delegate;
    }

    public static class LocalLock extends ReentrantLock
    {
        public LocalLock(String lockName)
        {
            super();
            this.name = lockName;
        }

        @Override
        public Collection<Thread> getQueuedThreads()
        {
            return super.getQueuedThreads();
        }

        public String getName()
        {
            return name;
        }

        private final String name;

    }

    public static class LocalDistributedDataStructureFactory implements DistributedDataStructureFactory
    {
        public LocalDistributedDataStructureFactory() {}

        @Override
        public DistributedSemaphore getSemaphore(String name, int initPermits) {
            return new LocalSemaphore(name, initPermits);
        }

        @Override
        public DistributedAtomicLong getAtomicLong(String name) {
            return new LocalAtomicLong(name);
        }

        @Override
        public Lock getLock(String name) {
            return new LocalLock(name);
        }

        @Override
        public Condition getCondition(Lock lock, String conditionName) {
            return lock.newCondition();
        }
    }

    /**
     * Returns the number of milliseconds since time given by
     * startNanoTime, which must have been previously returned from a
     * call to {@link System#nanoTime()}.
     */
    static long millisElapsedSince(long startNanoTime) {
        return NANOSECONDS.toMillis(System.nanoTime() - startNanoTime);
    }

    /**
     * Spin-waits until lock.hasQueuedThread(t) becomes true.
     */
    void waitForQueuedThread(PublicDistributedReentrantReadWriteLock lock, Thread t) {
        long startTime = System.nanoTime();
        while (!lock.hasQueuedThread(t)) {
            if (millisElapsedSince(startTime) > LONG_DELAY_MS)
                throw new AssertionFailedError("timed out");
            Thread.yield();
        }
        assertTrue(t.isAlive());
    }

    /**
     * Returns a new started daemon Thread running the given runnable.
     */
    Thread newStartedThread(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Waits for LONG_DELAY_MS milliseconds for the thread to
     * terminate (using {@link Thread#join(long)}), else interrupts
     * the thread (in the hope that it may terminate later) and fails.
     */
    void awaitTermination(Thread t) {
        awaitTermination(t, LONG_DELAY_MS);
    }

    /**
     * Waits for the specified time (in milliseconds) for the thread
     * to terminate (using {@link Thread#join(long)}), else interrupts
     * the thread (in the hope that it may terminate later) and fails.
     */
    void awaitTermination(Thread t, long timeoutMillis) {
        try {
            t.join(timeoutMillis);
        } catch (InterruptedException ie) {
            // threadUnexpectedException(ie);
            ie.printStackTrace();
        } finally {
            if (t.isAlive()) {
                t.interrupt();
                fail("Test timed out");
            }
        }
    }

    public abstract class CheckedRunnable implements Runnable {
        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
            } catch (Throwable t) {
                // threadUnexpectedException(t);
                t.printStackTrace();
            }
        }
    }

    public abstract class CheckedInterruptedRunnable implements Runnable {
        protected abstract void realRun() throws Throwable;

        public final void run() {
            try {
                realRun();
                fail("Thread should have thrown InterruptedException");
            } catch (InterruptedException success) {
            } catch (Throwable t) {
                // threadUnexpectedException(t);
                t.printStackTrace();
            }
        }
    }

    public static class ElapsedTimer
    {
        public ElapsedTimer(long durationMillis)
        {
            this.completedMillis = System.currentTimeMillis() + durationMillis;
        }

        public long remainingMillis()
        {
            return completedMillis - System.currentTimeMillis();
        }

        private final long completedMillis;
    }

    static final long LONG_DELAY_MS = 2000;
    /**
     * The number of elements to place in collections, arrays, etc.
     */
    public static final int SIZE = 20;

}
