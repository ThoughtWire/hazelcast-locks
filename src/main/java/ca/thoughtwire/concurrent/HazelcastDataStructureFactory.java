package ca.thoughtwire.concurrent;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.ISemaphore;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Factory for creating Hazelcast data structures.
 *
 * @author vanessa.williams
 */
public class HazelcastDataStructureFactory implements DistributedDataStructureFactory {

    public HazelcastDataStructureFactory(HazelcastInstance hazelcastInstance)
    {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public DistributedSemaphore getSemaphore(String name, int initPermits) {
        ISemaphore semaphore = hazelcastInstance.getSemaphore(name);
        semaphore.init(initPermits);
        return new HazelcastSemaphore(semaphore);
    }

    @Override
    public DistributedAtomicLong getAtomicLong(String name) {
        return new HazelcastAtomicLong(hazelcastInstance.getAtomicLong(name));
    }

    @Override
    public Lock getLock(String name) {
        return hazelcastInstance.getLock(name);
    }

    @Override
    public Condition getCondition(Lock lock, String conditionName) {
        return ((ILock)lock).newCondition(conditionName);
    }

    protected final HazelcastInstance hazelcastInstance;
}
