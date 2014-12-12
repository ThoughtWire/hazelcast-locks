
# Distributed Multiple Readers/Single Writer Locks using Hazelcast

This library uses Hazelcast's distributed data structures to create a java.util.concurrent.locks.ReadWriteLock
implementation. DistributedReentrantReadWriteLock is strongly reentrant (holders of write locks can acquire additional
read locks.)


A running Hazelcast cluster is required. The tests in the test folder demonstrate a simplistic way to
create a usable cluster of two nodes on localhost, but you can use any method you wish.

## Usage

Assuming you have a HazelcastInstance, usage is identical to usage of java.util.concurrent.locks.ReentrantReadWriteLock,
with the exception of creation of a new lock instance.

```java
// This can be a singleton, but additional instances aren't a problem.
DistributedLockService lockService =
    DistributedLockService.newHazelcastLockService(hazelcastInstance);

ReadWriteLock lock = lockService.getReentrantReadWriteLock("myLock");
lock.readLock().lock();
try {
    // do some stuff
}
finally {
    lock.readLock.unlock();
}

// shutting down the lock service cleans up
lockService.shutdown();
```

Note that not all methods are supported. For example, `newCondition()`, and untimed `tryLock()`
(due to limitations imposed by Hazelcast's `ICondition.await()` method.) These methods will throw
UnsupportedOperationException.