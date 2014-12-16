package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.HazelcastDataStructureFactory;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;


/**
 * @author vanessa.williams
 */
public class HazelcastReentrantReadWriteLockTest extends DistributedLockUtils {

    @BeforeClass
    public static void createGrids() {
		/* Suppress Hazelcast log output to standard error which does not appear to be suppressible via Agent Server's
		 * log4j.xml. */
//        ConsoleOutputSuppressor.suppressStandardError();
        final Config standardConfig1 = new ClasspathXmlConfig("hazelcast1.xml"),
                standardConfig2 = new ClasspathXmlConfig("hazelcast2.xml");

        standardConfig1.setProperty("hazelcast.operation.call.timeout.millis", "3000");
        standardConfig2.setProperty("hazelcast.operation.call.timeout.millis", "3000");
        grid1 = Hazelcast.newHazelcastInstance(standardConfig1);
        grid2 = Hazelcast.newHazelcastInstance(standardConfig2);

        dataStructureFactory1 = HazelcastDataStructureFactory.getInstance(grid1);
        dataStructureFactory2 = HazelcastDataStructureFactory.getInstance(grid2);

        lockService1 = new DistributedLockUtils().new PublicDistributedLockService(dataStructureFactory1);
        lockService2 = new DistributedLockUtils().new PublicDistributedLockService(dataStructureFactory2);
    }

    /**
     * write-locking and read-locking an unlocked lock succeed
     */
    @Test
    public void lockingUnlockedSucceeds()
    {
        System.out.println("lockingUnlockSucceeds");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
     * Note: you may have to adjust the property hazelcast.operation.call.timeout.millis to <2000 ms. Default is 120s!
     */
    @Test
    public void testWriteLockInterruptibly_Interruptible()
    {
        System.out.println("testWriteLockInterruptibly_Interruptible");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.writeLock().lockInterruptibly();
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * read-lockInterruptibly is interruptible
     * Note: you may have to adjust the property hazelcast.operation.call.timeout.millis to <2000 ms. Default is 120s!
     */
    @Test
    public void testReadLockInterruptibly_Interruptible()
    {
        System.out.println("testReadLockInterruptibly_Interruptible");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedInterruptedRunnable() {
            public void realRun() throws InterruptedException {
                lock.readLock().lockInterruptibly();
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * timed try read-lock is interruptible
     */
    @Test
    public void testTryReadLock_Interruptible()
    {
        System.out.println("testTryReadLock_Interruptible");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                lock.readLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * timed try write-lock is interruptible
     */
    @Test
    public void testTryWriteLock_Interruptible()
    {
        System.out.println("testTryWriteLock_Interruptible");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");

        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
            }});

        waitForQueuedThread(lock, t);
        t.interrupt();
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * write-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    @Test (expected = IllegalMonitorStateException.class)
    public void testWriteLock_MSIE()
    {
        System.out.println("testWriteLock_MSIE");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
         assertNotWriteLocked(lock);

        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * read-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    @Test (expected = IllegalMonitorStateException.class)
    public void testReadLock_MSIE()
    {
        System.out.println("testReadLock_MSIE");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        assertNotWriteLocked(lock);

        lock.readLock().unlock();
    }

    /**
     * getWriteHoldCount returns number of recursive holds
     */
    @Test
    public void testGetWriteHoldCount() {
        System.out.println("testGetWriteHoldCount");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testGetReadHoldCount");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testGetHoldCount");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testWriteTryLock");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testWriteTryLockWhenLocked");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                try {
                    assertFalse(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                } catch (InterruptedException ignore) {
                    System.out.println("Caught an interrupted exception");
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
        System.out.println("testReadTryLockWhenLocked");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testMultipleReadLocks");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testWriteAfterReadLock");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testWriteAfterMultipleReadLocks");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.readLock().lock();
        lock.readLock().lock();
        assertEquals(2, lock.getReadHoldCount());
        Thread t1 = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                lock.readLock().lock();
                assertEquals(1, lock.getReadHoldCount());
                lock.readLock().unlock();
            }});
        waitForQueuedThread(lock, t1);
        awaitTermination(t1, 3 * LONG_DELAY_MS);

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
        awaitTermination(t2, 3 * LONG_DELAY_MS);
        assertNotWriteLocked(lock);
    }

    /**
     * Readlocks succeed only after a writing thread unlocks
     */
    @Test
    public void testReadAfterWriteLock() {
        System.out.println("testReadAfterWriteLock");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        awaitTermination(t1, 3 * LONG_DELAY_MS);
        awaitTermination(t2, 3 * LONG_DELAY_MS);
        assertNotWriteLocked(lock);
    }

    /**
     * Read trylock succeeds if write locked by current thread
     */
    @Test
    public void testReadHoldingWriteLock() throws InterruptedException {
        System.out.println("testReadHoldingWriteLock");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        System.out.println("testReadHoldingWriteLock2");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        assertWriteLockedByMe(lock);
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

        lock.readLock().lock();
        lock.readLock().unlock();
        lock.writeLock().unlock();
        awaitTermination(t1, 3 * LONG_DELAY_MS);
        awaitTermination(t2, 3 * LONG_DELAY_MS);
        assertNotWriteLocked(lock);
    }

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    @Test
    public void testReadHoldingWriteLock3() {
        System.out.println("testReadHoldingWriteLock3");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        assertWriteLockedByMe(lock);
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
        lock.readLock().lock();
        lock.readLock().unlock();
        lock.writeLock().unlock();
        awaitTermination(t1, 3 * LONG_DELAY_MS);
        awaitTermination(t2, 3 * LONG_DELAY_MS);
        assertNotWriteLocked(lock);
    }

    /**
     * Write lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    @Test
    public void testWriteHoldingWriteLock4() {
        System.out.println("testWriteHoldingWriteLock4");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
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
        awaitTermination(t1, 3 * LONG_DELAY_MS);
        awaitTermination(t2, 3 * LONG_DELAY_MS);
        assertNotWriteLocked(lock);
    }

    /**
     * Read tryLock succeeds if readlocked but not writelocked
     */
    @Test
    public void testTryLockWhenReadLocked() {
        System.out.println("testTryLockWhenReadLocked");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                lock.readLock().unlock();
            }});

        waitForQueuedThread(lock,t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * write tryLock fails when readlocked
     */
    @Test
    public void testWriteTryLockWhenReadLocked() {
        System.out.println("testWriteTryLockWhenReadLocked");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(lock.writeLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
            }});

        waitForQueuedThread(lock,t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * write timed tryLock times out if locked
     */
    @Test
    public void testWriteTryLock_Timeout() {
        System.out.println("testWriteTryLock_Timeout");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                assertFalse(lock.writeLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    /**
     * read timed tryLock times out if write-locked
     */
    @Test
    public void testReadTryLock_Timeout() {
        System.out.println("testReadTryLock_Timeout");
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockService1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                assertFalse(lock.readLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        waitForQueuedThread(lock, t);
        awaitTermination(t, 3 * LONG_DELAY_MS);
        assertTrue(((PublicDistributedReentrantReadWriteLock.WriteLock) lock.writeLock()).isHeldByCurrentThread());
        lock.writeLock().unlock();
        assertNotWriteLocked(lock);
    }

    @AfterClass
    public static void tearDown()
    {
        lockService1.shutdown();
        lockService2.shutdown();
        grid1.shutdown();
        grid2.shutdown();
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

    private static HazelcastInstance grid1, grid2;
    private static HazelcastDataStructureFactory dataStructureFactory1, dataStructureFactory2;
    private static PublicDistributedLockService lockService1, lockService2;

}
