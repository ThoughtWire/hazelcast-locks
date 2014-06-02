package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.DistributedDataStructureFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class DistributedReadWriteLockTest extends DistributedLockUtils {

    @Before
    public void setUp()
    {
        grid = new DistributedLockUtils.LocalDistributedDataStructureFactory();
        lockFactory = new DistributedLockFactory(grid);
    }

    /**
     * write-locking and read-locking an unlocked lock succeed
     */
    @Test
    public void lockingUnlockedSucceeds()
    {
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.writeLock().lock();
        assertWriteLocked(lock);
        assertNotReadLocked(lock);
        assertEquals(1, lock.getWriteCount());

        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);
        assertEquals(0, lock.getWriteCount());

        lock.readLock().lock();
        assertNotWriteLocked(lock);
        assertReadLocked(lock);
        assertEquals(1, lock.getReadCount());

        lock.readLock().unlock();
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);
        assertEquals(0, lock.getReadCount());
    }

    /**
     * write-lockInterruptibly is interruptible
     */
    @Test
    public void testWriteLockInterruptibly_Interruptible()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

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
    @Test
    public void testReadLockInterruptibly_Interruptible()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

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
    @Test
    public void testTryReadLock_Interruptible()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

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
    @Test
    public void testTryWriteLock_Interruptible()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

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
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.writeLock().unlock();
    }

    /**
     * read-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    @Test (expected = IllegalMonitorStateException.class)
    public void testReadLock_MSIE()
    {
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.readLock().unlock();
    }

    /**
     * read-locking a readlocked lock throws IllegalThreadStateException
     */
    @Test (expected = IllegalThreadStateException.class)
    public void testTwoNestedReads_ITSE()
    {
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.readLock().lock();
        assertReadLocked(lock);
        try {
            lock.readLock().lock();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * write-locking a writelocked lock throws IllegalThreadStateException
     */
    @Test (expected = IllegalThreadStateException.class)
    public void testTwoNestedWrites_ITSE()
    {
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.writeLock().lock();
        assertWriteLocked(lock);
        try {
            lock.writeLock().lock();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * upgrading a readlocked lock throws IllegalThreadStateException
     */
    @Test (expected = IllegalThreadStateException.class)
    public void upgradeFails_ITSE()
    {
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.readLock().lock();
        assertReadLocked(lock);
        try {
            lock.writeLock().lock();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * downgrading a writelocked lock throws IllegalThreadStateException
     */
    @Test (expected = IllegalThreadStateException.class)
    public void downgradeFails_ITSE()
    {
        PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        assertNotWriteLocked(lock);
        assertNotReadLocked(lock);

        lock.writeLock().lock();
        assertWriteLocked(lock);
        try {
            lock.readLock().lock();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Multiple threads can hold a read lock when not write-locked
     */
    @Test
    public void testMultipleReaders()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                lock.readLock().lock();
                lock.readLock().unlock();
            }});

        awaitTermination(t);
        lock.readLock().unlock();
    }

    /**
     * A writelock succeeds only after a reading thread unlocks
     */
    @Test
    public void testWriteAfterReadLock()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                assertEquals(1, lock.getReadCount());
                lock.writeLock().lock();
                assertEquals(0, lock.getReadCount());
                lock.writeLock().unlock();
            }});
        waitForQueuedThread(lock, t);
        assertNotWriteLocked(lock);
        assertEquals(1, lock.getReadCount());
        lock.readLock().unlock();
        assertEquals(0, lock.getReadCount());
        awaitTermination(t);
        assertNotWriteLocked(lock);

    }

    /**
     * Readlocks succeed only after a writing thread unlocks
     */
    @Test
    public void testReadAfterWriteLock()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");

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
//        releaseWriteLock(lock);
        lock.writeLock().unlock();
        awaitTermination(t1);
        awaitTermination(t2);
    }

    /**
     * Same thread can get readlocks on multiple named locks.
     */
    @Test
    public void multipleNamedReadLocks()
    {
        final PublicDistributedReadWriteLock lock1 =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        final PublicDistributedReadWriteLock lock2 =
                new PublicDistributedReadWriteLock(grid, "testLock2");

        lock1.readLock().lock();
        assertReadLocked(lock1);
        assertEquals(1, lock1.getReadCount());
        lock2.readLock().lock();
        assertReadLocked(lock2);
        assertEquals(1, lock2.getReadCount());

        lock1.readLock().unlock();
        assertNotReadLocked(lock1);
        assertEquals(0, lock1.getReadCount());

        lock2.readLock().unlock();
        assertNotReadLocked(lock2);
        assertEquals(0, lock2.getReadCount());
    }

    /**
     * Same thread can get writeLocks on multiple named locks.
     */
    @Test
    public void multipleNamedWriteLocks()
    {
        final PublicDistributedReadWriteLock lock1 =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        final PublicDistributedReadWriteLock lock2 =
                new PublicDistributedReadWriteLock(grid, "testLock2");

        lock1.writeLock().lock();
        assertWriteLocked(lock1);
        assertEquals(1, lock1.getWriteCount());
        lock2.writeLock().lock();
        assertWriteLocked(lock2);
        assertEquals(1, lock2.getWriteCount());

        lock1.writeLock().unlock();
        assertNotWriteLocked(lock1);
        assertEquals(0, lock1.getWriteCount());

        lock2.writeLock().unlock();
        assertNotWriteLocked(lock2);
        assertEquals(0, lock2.getWriteCount());
        
    }

    /**
     * write-tryLock on an unlocked lock succeeds
     */
    @Test
    public void testWriteTryLock()  {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        assertTrue(lock.writeLock().tryLock());
        assertWriteLockedByMe(lock);
    }

    /**
     * write-tryLock fails if locked
     */
    @Test
    public void testWriteTryLockWhenLocked() {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                assertFalse(lock.writeLock().tryLock());
            }});

        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * read-tryLock fails if locked
     */
    @Test
    public void testReadTryLockWhenLocked() {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                assertFalse(lock.readLock().tryLock());
            }});

        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * timed write-tryLock is interruptible
     */
    @Test
    public void testWriteTryLock_Interruptible() {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * timed read-tryLock is interruptible
     */
    @Test
    public void testReadTryLock_Interruptible() {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock1");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.readLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t);
        lock.writeLock().unlock();
    }
    
    @Test (expected = UnsupportedOperationException.class)
    public void readNewConditionNotSupported()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        lock.readLock().newCondition();            
    }

    @Test (expected = UnsupportedOperationException.class)
    public void writeNewConditionNotSupported()
    {
        final PublicDistributedReadWriteLock lock =
                new PublicDistributedReadWriteLock(grid, "testLock");
        lock.writeLock().newCondition();            
    }
    
    void assertNotWriteLocked(PublicDistributedReadWriteLock lock)
    {
        assertFalse(lock.isWriteLocked());
    }

    void assertWriteLocked(PublicDistributedReadWriteLock lock)
    {
        assertTrue(lock.isWriteLocked());
    }

    void assertWriteLockedByMe(PublicDistributedReadWriteLock lock)
    {
        assertTrue(lock.isWriteLocked());
        assertEquals(Thread.currentThread().getId(),lock.getOwner());
    }

    void assertReadLocked(PublicDistributedReadWriteLock lock)
    {
        assertTrue(lock.isReadLocked());
    }

    void assertNotReadLocked(PublicDistributedReadWriteLock lock)
    {
        assertFalse(lock.isReadLocked());
    }


    DistributedDataStructureFactory grid;
    DistributedLockFactory lockFactory;
}
