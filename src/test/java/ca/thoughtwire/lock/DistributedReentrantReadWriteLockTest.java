package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.DistributedDataStructureFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author vanessa.williams
 *
 * Note: the "interruptible" methods can't be tested this way because the behaviour of Hazelcast
 * when threads holding locks are interrupted is different than {@link java.util.concurrent.locks.Lock}.
 * The implementation only works for the Hazelcast version of interruption handling.
 */
public class DistributedReentrantReadWriteLockTest extends DistributedLockUtils {

    @BeforeClass
    public static void setUp()
    {
        grid = new LocalDistributedDataStructureFactory();
        lockService = new DistributedLockService(grid);
    }

    /**
     * write-locking and read-locking an unlocked lock succeed
     */
    @Test
    public void lockingUnlockedSucceeds()
    {
        PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        assertNotWriteLocked(lock);

        lock.writeLock().lock();
        assertWriteLockedByMe(lock);
        assertHoldCount(lock, 1);

        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
        assertHoldCount(lock, 0);

        lock.readLock().lock();
        assertNotWriteLocked(lock);
        assertHoldCount(lock, 1);

        lock.readLock().unlock();
        assertNotWriteLocked(lock);
        assertHoldCount(lock, 0);
    }

    /**
     * write-lockInterruptibly is interruptible
     */
    @Ignore
    @Test
    public void testWriteLockInterruptibly_Interruptible()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.writeLock().lockInterruptibly();
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * read-lockInterruptibly is interruptible
     */
    @Ignore
    @Test
    public void testReadLockInterruptibly_Interruptible()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.readLock().lockInterruptibly();
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * timed try read-lock is interruptible
     */
    @Ignore
    @Test
    public void testTryReadLock_Interruptible()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.readLock().tryLock(4 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * timed try write-lock is interruptible
     */
    @Ignore
    @Test
    public void testTryWriteLock_Interruptible()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.writeLock().tryLock(4 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * write-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    @Test (expected = IllegalMonitorStateException.class)
    public void testWriteLock_MSIE()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        assertNotWriteLocked(lock);

        lock.writeLock().unlock();
    }

    /**
     * read-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    @Test (expected = IllegalMonitorStateException.class)
    public void testReadLock_MSIE()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        assertNotWriteLocked(lock);

        lock.readLock().unlock();
    }

    /**
     * getWriteHoldCount returns number of recursive holds
     */
    @Test
    public void testGetWriteHoldCount() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        for (int i = 1; i <= SIZE; i++) {
            lock.writeLock().lock();
            assertEquals(i,lock.getWriteHoldCount());
        }
        for (int i = SIZE; i > 0; i--) {
            lock.writeLock().unlock();
            assertEquals(i-1,lock.getWriteHoldCount());
        }
        assertNotWriteLocked(lock);
    }

    /**
     * getReadHoldCount returns number of recursive holds
     */
    @Test
    public void testGetReadHoldCount() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        for (int i = 1; i <= SIZE; i++) {
            lock.readLock().lock();
            assertEquals(i,lock.getReadHoldCount());
        }
        for (int i = SIZE; i > 0; i--) {
            lock.readLock().unlock();
            assertEquals(i-1,lock.getReadHoldCount());
        }
    }

    /**
     * writelock.getHoldCount returns number of recursive holds
     */
    @Test
    public void testGetHoldCount() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        for (int i = 1; i <= SIZE; i++) {
            lock.writeLock().lock();
            assertEquals(i,((DistributedReentrantReadWriteLock.WriteLock)lock.writeLock()).getHoldCount());
        }
        for (int i = SIZE; i > 0; i--) {
            lock.writeLock().unlock();
            assertEquals(i-1,((DistributedReentrantReadWriteLock.WriteLock)lock.writeLock()).getHoldCount());
        }
        assertNotWriteLocked(lock);
    }

    /**
     * timed write-tryLock on an unlocked lock succeeds
     */
    @Test
    public void testWriteTryLock() throws InterruptedException {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        assertTrue(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        assertWriteLockedByMe(lock);
        assertTrue(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        assertWriteLockedByMe(lock);
        lock.writeLock().unlock();
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }
    /**
     * timed write-tryLock fails if locked
     */
    @Test
    public void testWriteTryLockWhenLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                try {
                    assertFalse(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * timed read-tryLock fails if locked
     */
    @Test
    public void testReadTryLockWhenLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                try {
                    assertFalse(lock.readLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * Multiple threads can hold a read lock when not write-locked
     */
    @Test
    public void testMultipleReadLocks() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                lock.readLock().unlock();
                assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                lock.readLock().unlock();
                lock.readLock().lock();
                lock.readLock().unlock();
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * A writelock succeeds only after a reading thread unlocks
     */
    @Test
    public void testWriteAfterReadLock() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.writeLock().lock();
                assertEquals(0, lock.getReadHoldCount());
                lock.writeLock().unlock();
            }});
        waitForQueuedThread(lock, t);
        assertNotWriteLocked(lock);
        assertEquals(1, lock.getReadHoldCount());
        lock.readLock().unlock();
        assertEquals(0, lock.getReadHoldCount());
        awaitTermination(t);
        assertNotWriteLocked(lock);
    }

    /**
     * A writelock succeeds only after reading threads unlock
     */
    @Test
    public void testWriteAfterMultipleReadLocks() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.readLock().lock();
        lock.readLock().lock();
        assertEquals(2, lock.getReadHoldCount());
        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.readLock().lock();
                assertEquals(1, lock.getReadHoldCount());
                lock.readLock().unlock();
            }});
        awaitTermination(t1);

        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.writeLock().lock();
                assertEquals(1, lock.getWriteHoldCount());
                lock.writeLock().unlock();
            }});
        waitForQueuedThread(lock, t2);
        assertNotWriteLocked(lock);
        assertEquals(2, lock.getReadHoldCount());
        lock.readLock().unlock();
        lock.readLock().unlock();
        assertEquals(0, lock.getReadHoldCount());
        awaitTermination(t2);
        assertNotWriteLocked(lock);
    }

    /**
     * Readlocks succeed only after a writing thread unlocks
     */
    @Test
    public void testReadAfterWriteLock() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.readLock().lock();
                lock.readLock().unlock();
            }});
        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.readLock().lock();
                lock.readLock().unlock();
            }});

        waitForQueuedThread(lock, t1);
        waitForQueuedThread(lock, t2);
        lock.writeLock().unlock();
        awaitTermination(t1);
        awaitTermination(t2);
        assertNotWriteLocked(lock);
    }

    /**
     * Read trylock succeeds if write locked by current thread
     */
    @Test
    public void testReadHoldingWriteLock() throws InterruptedException {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        lock.readLock().unlock();
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting for readlock
     */
    @Test
    public void testReadHoldingWriteLock2() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        lock.readLock().lock();
        lock.readLock().unlock();

        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.readLock().lock();
                lock.readLock().unlock();
            }});
        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.readLock().lock();
                lock.readLock().unlock();
            }});

        waitForQueuedThread(lock, t1);
        waitForQueuedThread(lock, t2);
        assertWriteLockedByMe(lock);
        lock.readLock().lock();
        lock.readLock().unlock();
        lock.writeLock().unlock();
        awaitTermination(t1);
        awaitTermination(t2);
        assertNotWriteLocked(lock);
    }

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    @Test
    public void testReadHoldingWriteLock3() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        lock.readLock().lock();
        lock.readLock().unlock();

        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.writeLock().lock();
                lock.writeLock().unlock();
            }});
        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.writeLock().lock();
                lock.writeLock().unlock();
            }});

        waitForQueuedThread(lock, t1);
        waitForQueuedThread(lock, t2);
        assertWriteLockedByMe(lock);
        lock.readLock().lock();
        lock.readLock().unlock();
        assertWriteLockedByMe(lock);
        lock.writeLock().unlock();
        awaitTermination(t1);
        awaitTermination(t2);
        assertNotWriteLocked(lock);
    }

    /**
     * Write lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    @Test
    public void testWriteHoldingWriteLock4() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        lock.writeLock().lock();
        lock.writeLock().unlock();

        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.writeLock().lock();
                lock.writeLock().unlock();
            }});
        Thread t2 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.writeLock().lock();
                lock.writeLock().unlock();
            }});

        waitForQueuedThread(lock, t1);
        waitForQueuedThread(lock, t2);
        assertWriteLockedByMe(lock);
        assertEquals(1, lock.getWriteHoldCount());
        lock.writeLock().lock();
        assertWriteLockedByMe(lock);
        assertEquals(2, lock.getWriteHoldCount());
        lock.writeLock().unlock();
        assertWriteLockedByMe(lock);
        assertEquals(1, lock.getWriteHoldCount());
        lock.writeLock().unlock();
        awaitTermination(t1);
        awaitTermination(t2);
        assertNotWriteLocked(lock);
    }

    /**
     * Read tryLock succeeds if readlocked but not writelocked
     */
    @Test
    public void testTryLockWhenReadLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                lock.readLock().unlock();
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 2 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * write tryLock fails when readlocked
     */
    @Test
    public void testWriteTryLockWhenReadLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(lock.writeLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 2 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * write timed tryLock times out if locked
     */
    @Test
    public void testWriteTryLock_Timeout() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                assertFalse(lock.writeLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * read timed tryLock times out if write-locked
     */
    @Test
    public void testReadTryLock_Timeout() {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                assertFalse(lock.readLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t);
        assertTrue(((PublicDistributedReentrantReadWriteLock.WriteLock)lock.writeLock()).isHeldByCurrentThread());
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testUntimedTryReadLockNotSupported() throws InterruptedException {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.readLock().tryLock();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void testUntimedTryWriteLockNotSupported() throws InterruptedException {
        final PublicDistributedReentrantReadWriteLock lock =
                new PublicDistributedReentrantReadWriteLock(lockService, "testLock");
        lock.writeLock().tryLock();
    }

    @AfterClass
    public static void tearDown()
    {
        lockService.shutdown();
    }

    private void assertNotWriteLocked(PublicDistributedReentrantReadWriteLock lock)
    {
        assertFalse(lock.isWriteLocked());
    }

    private void assertWriteLocked(PublicDistributedReentrantReadWriteLock lock)
    {
        assertTrue(lock.isWriteLocked());
    }

    private void assertWriteLockedByMe(PublicDistributedReentrantReadWriteLock lock)
    {
        assertTrue(lock.isWriteLocked());
        assertTrue(lock.isHeldByCurrentThread());
    }

    private void assertWriteLockedByOther(PublicDistributedReentrantReadWriteLock lock)
    {
        assertTrue(lock.isWriteLocked());
        assertFalse(lock.isHeldByCurrentThread());
    }

    private void assertHoldCount(PublicDistributedReentrantReadWriteLock lock, int count)
    {
        assertEquals(count, lock.getHoldCount());
    }

    static DistributedDataStructureFactory grid;
    static DistributedLockService lockService;

}
