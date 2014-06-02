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
public class DistributedReentrantReadWriteLockIT extends DistributedLockUtils {

    @BeforeClass
    public static void createGrids()
    {
		/* Suppress Hazelcast log output to standard error which does not appear to be suppressible via Agent Server's
		 * log4j.xml. */
//        ConsoleOutputSuppressor.suppressStandardError();
        final Config standardConfig1 = new ClasspathXmlConfig("hazelcast1.xml"),
                standardConfig2 = new ClasspathXmlConfig("hazelcast2.xml");

        standardConfig1.setProperty("hazelcast.operation.call.timeout.millis", "4000");
        standardConfig2.setProperty("hazelcast.operation.call.timeout.millis", "4000");
        grid1 = Hazelcast.newHazelcastInstance(standardConfig1);
        grid2 = Hazelcast.newHazelcastInstance(standardConfig2);

        dataStructureFactory1 = new HazelcastDataStructureFactory(grid1);
        dataStructureFactory2 = new HazelcastDataStructureFactory(grid2);

        lockFactory1 = new PublicDistributedLockFactory(dataStructureFactory1);
        lockFactory2 = new PublicDistributedLockFactory(dataStructureFactory2);
    }

    /**
     * write-locking and read-locking an unlocked lock succeed
     */
    @Test
    public void lockingUnlockedSucceeds()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
     * write-unlocking an unlocked lock throws IllegalMonitorStateException
     */
    @Test (expected = IllegalMonitorStateException.class)
    public void testWriteLock_MSIE()
    {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        assertNotWriteLocked(lock);

        lock.readLock().unlock();
    }

    /**
     * getWriteHoldCount returns number of recursive holds
     */
    @Test
    public void testGetWriteHoldCount() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        for (int i = 1; i <= SIZE; i++) {
            lock.writeLock().lock();
            assertEquals(i,lock.getWriteHoldCount());
        }
        for (int i = SIZE; i > 0; i--) {
            lock.writeLock().unlock();
            assertEquals(i-1,lock.getWriteHoldCount());
        }
    }

    /**
     * getReadHoldCount returns number of recursive holds
     */
    @Test
    public void testGetReadHoldCount() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        for (int i = 1; i <= SIZE; i++) {
            lock.writeLock().lock();
            assertEquals(i,((DistributedReentrantReadWriteLock.WriteLock)lock.writeLock()).getHoldCount());
        }
        for (int i = SIZE; i > 0; i--) {
            lock.writeLock().unlock();
            assertEquals(i-1,((DistributedReentrantReadWriteLock.WriteLock)lock.writeLock()).getHoldCount());
        }
    }

    /**
     * timed write-tryLock on an unlocked lock succeeds
     */
    @Test
    public void testWriteTryLock() throws InterruptedException {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        assertTrue(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        assertWriteLockedByMe(lock);
        assertTrue(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        assertWriteLockedByMe(lock);
        lock.writeLock().unlock();
        lock.writeLock().unlock();
    }
    /**
     * timed write-tryLock fails if locked
     */
    @Test
    public void testWriteTryLockWhenLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                try {
                    assertFalse(lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }});

        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
    }

    /**
     * timed read-tryLock fails if locked
     */
    @Test
    public void testReadTryLockWhenLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() {
                try {
                    assertFalse(lock.readLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }});

        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.writeLock().unlock();
    }

    /**
     * Multiple threads can hold a read lock when not write-locked
     */
    @Test
    public void testMultipleReadLocks() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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

        awaitTermination(t, 3 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * A writelock succeeds only after a reading thread unlocks
     */
    @Test
    public void testWriteAfterReadLock() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
        awaitTermination(t1, 2 * LONG_DELAY_MS);
        awaitTermination(t2, 2 * LONG_DELAY_MS);
    }

    /**
     * Read trylock succeeds if write locked by current thread
     */
    @Test
    public void testReadHoldingWriteLock() throws InterruptedException {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        lock.readLock().unlock();
        lock.writeLock().unlock();
    }

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting for readlock
     */
    @Test
    public void testReadHoldingWriteLock2() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
    }

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    @Test
    public void testReadHoldingWriteLock3() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
    }

    /**
     * Write lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    @Test
    public void testWriteHoldingWriteLock4() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
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
    }

    /**
     * Read tryLock succeeds if readlocked but not writelocked
     */
    @Test
    public void testTryLockWhenReadLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertTrue(lock.readLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
                lock.readLock().unlock();
            }});

        awaitTermination(t);
        lock.readLock().unlock();
    }

    /**
     * write tryLock fails when readlocked
     */
    @Test
    public void testWriteTryLockWhenReadLocked() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.readLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                assertFalse(lock.writeLock().tryLock(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
            }});

        awaitTermination(t, 2 * LONG_DELAY_MS);
        lock.readLock().unlock();
    }

    /**
     * write timed tryLock times out if locked
     */
    @Test
    public void testWriteTryLock_Timeout() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                assertFalse(lock.writeLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        awaitTermination(t);
        lock.writeLock().unlock();
    }

    /**
     * read timed tryLock times out if write-locked
     */
    @Test
    public void testReadTryLock_Timeout() {
        final PublicDistributedReentrantReadWriteLock lock =
                (PublicDistributedReentrantReadWriteLock)lockFactory1.getReentrantReadWriteLock("testLock");
        lock.writeLock().lock();
        Thread t = newStartedThread(new CheckedRunnable() {
            public void realRun() throws InterruptedException {
                long startTime = System.nanoTime();
                long timeoutMillis = 10;
                assertFalse(lock.readLock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS));
                assertTrue(millisElapsedSince(startTime) >= timeoutMillis);
            }});

        awaitTermination(t);
        assertTrue(((PublicDistributedReentrantReadWriteLock.WriteLock)lock.writeLock()).isHeldByCurrentThread());
        lock.writeLock().unlock();
    }

    @AfterClass
    public static void tearDown()
    {
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
    private static PublicDistributedLockFactory lockFactory1, lockFactory2;

}
