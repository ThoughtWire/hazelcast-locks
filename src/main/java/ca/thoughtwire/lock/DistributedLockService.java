package ca.thoughtwire.lock;

import ca.thoughtwire.concurrent.DistributedDataStructureFactory;
import ca.thoughtwire.concurrent.HazelcastDataStructureFactory;
import com.hazelcast.core.HazelcastInstance;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A factory for distributed locks.
 *
 * @author vanessa.williams
 */
public class DistributedLockService {

    /**
     * Convenience static factory method for creating a lock factory using Hazelcast.
     * A shortcut for new DistributedLockService(new HazelcastDataStructureFactory(hazelcastInstance))).
     *
     * @param hazelcastInstance  the grid instance
     * @return A DistributedLockService based on a HazelcastDataStructureFactory.
     */
    public static DistributedLockService newHazelcastLockService(HazelcastInstance hazelcastInstance)
    {
        if (hazelcastInstance == null)
        {
            throw new IllegalArgumentException("HazelcastInstance argument is required.");
        }
        return new DistributedLockService(new HazelcastDataStructureFactory(hazelcastInstance));
    }

    /**
     * Constructor.
     *
     * @param distributedDataStructureFactory factory for creating distributed semaphores and atomic primitives
     */
    public DistributedLockService(DistributedDataStructureFactory distributedDataStructureFactory)
    {
        if (distributedDataStructureFactory == null)
        {
            throw new IllegalArgumentException("DistributedDataStructureFactory argument is required.");
        }
        this.distributedDataStructureFactory = distributedDataStructureFactory;
    }

    /**
     * @param lockName name of the lock
     * @return a distributed readers-writer lock
     */
    public ReadWriteLock getReadWriteLock(String lockName)
    {
        if (threadLocks.get().containsKey(lockName))
        {
            return threadLocks.get().get(lockName);
        }
        else {
            DistributedReadWriteLock lock = new DistributedReadWriteLock(distributedDataStructureFactory, lockName);
            threadLocks.get().put(lockName, lock);
            return lock;
        }
    }

    /**
     * @param lockName name of the lock
     * @return a re-entrant distributed readers-writers lock
     */
    public ReadWriteLock getReentrantReadWriteLock(String lockName)
    {
        if (threadReentrantLocks.get().containsKey(lockName))
        {
            return threadReentrantLocks.get().get(lockName);
        }
        else {
            DistributedReentrantReadWriteLock lock = new DistributedReentrantReadWriteLock(distributedDataStructureFactory, lockName);
            threadReentrantLocks.get().put(lockName, lock);
            return lock;
        }

    }

    public static void shutdown()
   	{
   		threadLocks.remove();
   		threadReentrantLocks.remove();
   	}

    static final ThreadLocal<Map<String, DistributedReadWriteLock>> threadLocks = new ThreadLocal<Map<String, DistributedReadWriteLock>>() {
        @Override
        protected Map<String, DistributedReadWriteLock> initialValue() {
            return new HashMap<String, DistributedReadWriteLock>();
        }
    };

    static final ThreadLocal<Map<String, DistributedReentrantReadWriteLock>> threadReentrantLocks =
            new ThreadLocal<Map<String, DistributedReentrantReadWriteLock>>() {
        @Override
        protected Map<String, DistributedReentrantReadWriteLock> initialValue() {
            return new HashMap<String, DistributedReentrantReadWriteLock>();
        }
    };

    final DistributedDataStructureFactory distributedDataStructureFactory;
}
