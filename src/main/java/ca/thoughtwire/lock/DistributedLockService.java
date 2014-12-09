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
     * @return a re-entrant distributed readers-writers lock
     */
    public ReadWriteLock getReentrantReadWriteLock(String lockName)
    {
        if (THREAD_LOCKS.containsKey(lockName))
        {
            return THREAD_LOCKS.get(lockName);
        }
        else {
            DistributedReentrantReadWriteLock lock =
                    new DistributedReentrantReadWriteLock(distributedDataStructureFactory, lockName);
            THREAD_LOCKS.put(lockName, lock);
            return lock;
        }

    }


    static final Map<String, DistributedReentrantReadWriteLock> THREAD_LOCKS =
            new HashMap<String, DistributedReentrantReadWriteLock>();


    final DistributedDataStructureFactory distributedDataStructureFactory;
}
