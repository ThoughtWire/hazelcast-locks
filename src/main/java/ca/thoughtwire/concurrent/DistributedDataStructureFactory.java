package ca.thoughtwire.concurrent;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Interface for creating distributed data structures.
 *
 * @author vanessa.williams
 */
public interface DistributedDataStructureFactory {

    /**
     * @param name the semaphore name
     * @param initPermits the number of initial permits
     * @return a named semaphore, created & initialized with requested permits if necessary.
     */
    DistributedSemaphore getSemaphore(String name, int initPermits);

    /**
     * @param name the atomic long name
     * @return a named atomic long
     */
    DistributedAtomicLong getAtomicLong(String name);

    /**
     * @param name the lock name
     * @return a named lock
     */
    Lock getLock(String name);

    /**
     * @param lock a lock
     * @param conditionName a name for the condition
     * @return a new condition
     */
    Condition getCondition(Lock lock, String conditionName);

}
