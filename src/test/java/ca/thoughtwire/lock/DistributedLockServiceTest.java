package ca.thoughtwire.lock;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.locks.ReadWriteLock;

import static ca.thoughtwire.lock.DistributedLockUtils.LocalDistributedDataStructureFactory;
import static org.junit.Assert.assertTrue;

/**
 * @author vanessa.williams
 */
public class DistributedLockServiceTest {

    @Before
    public void setUp()
    {
        this.lockService =
                new DistributedLockService(new LocalDistributedDataStructureFactory());
    }

    /**
     * One lock per name per thread.
     */
    @Test
    public void oneLockObjectPerName()
    {
        ReadWriteLock lock1 = lockService.getReentrantReadWriteLock("testLock");
        ReadWriteLock lock2 = lockService.getReentrantReadWriteLock("testLock");
        assertTrue(lock1 == lock2);

        lock1.readLock().lock();
        lock2.readLock().lock();

        lock1.readLock().unlock();
        lock2.readLock().unlock();
    }

    /**
     * Two factories return the same lock.
     */
    @Test
    public void testMultipleServices()
    {
        DistributedLockService lockService2 =
                new DistributedLockService(new LocalDistributedDataStructureFactory());

        ReadWriteLock lock1 = lockService.getReentrantReadWriteLock("testLock");
        ReadWriteLock lock2 = lockService2.getReentrantReadWriteLock("testLock");
        assertTrue(lock1 == lock2);
        lock1.readLock().lock();
        lock2.readLock().lock();

        lock1.readLock().unlock();
        lock2.readLock().unlock();

    }

    DistributedLockService lockService;
}
