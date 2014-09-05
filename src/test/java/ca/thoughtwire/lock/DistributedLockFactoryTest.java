package ca.thoughtwire.lock;

import com.hazelcast.core.HazelcastInstance;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.ReadWriteLock;

import static ca.thoughtwire.lock.DistributedLockUtils.LocalDistributedDataStructureFactory;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author vanessa.williams
 */
public class DistributedLockFactoryTest {

    @Before
    public void setUp()
    {
        this.lockFactory =
                new DistributedLockFactory(new LocalDistributedDataStructureFactory());
    }

    @Test
    public void factoryMethodSucceeds()
    {
        EasyMockSupport mockManager = new EasyMockSupport();
        HazelcastInstance hazelcastInstance = mockManager.createNiceMock(HazelcastInstance.class);
        DistributedLockFactory lockFactory = DistributedLockFactory.newHazelcastLockFactory(hazelcastInstance);
        assertFalse(lockFactory == null);
    }

    /**
     * One lock per name per thread.
     */
    @Test
    public void oneLockObjectPerNamePerThread()
    {
        ReadWriteLock lock1 = lockFactory.getReadWriteLock("testLock");
        ReadWriteLock lock2 = lockFactory.getReadWriteLock("testLock");
        assertTrue(lock1 == lock2);

        lock1.readLock().lock();
        try {
            lock2.readLock().lock();
            fail("Thread should throw IllegalThreadStateException");
        } catch (IllegalThreadStateException success) {
        } finally {
            lock1.readLock().unlock();
        }
    }

    /**
     * Two factories return the same lock.
     */
    @Test
    public void testMultipleFactories()
    {
        DistributedLockFactory lockFactory2 =
                new DistributedLockFactory(new LocalDistributedDataStructureFactory());

        ReadWriteLock lock1 = lockFactory.getReadWriteLock("testLock");
        ReadWriteLock lock2 = lockFactory2.getReadWriteLock("testLock");
        assertTrue(lock1 == lock2);
        lock1.readLock().lock();
        try {
            lock2.readLock().lock();
            fail("Thread should throw IllegalThreadStateException");
        } catch (IllegalThreadStateException success) {
        } finally {
            lock1.readLock().unlock();
        }
    }

    DistributedLockFactory lockFactory;
}
