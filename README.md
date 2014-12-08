
# Distributed Multiple Readers/Single Writer Locks using Hazelcast

This library uses Hazelcast's distributed data structures to create two java.util.concurrent.locks.ReadWriteLock
implementations. DistributedReadWriteLock is not reentrant. DistributedReentrantReadWriteLock is strongly
reentrant (holders of write locks can acquire additional read locks.)


A running Hazelcast cluster is required. The integration tests in the test folder demonstrate a simplistic way to
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

// On application shutdown, this will clean up ThreadLocals. If you're not
// running in a container like Tomcat and don't plan to restart the app
// without restarting the container, you can skip this.
// Not doing this only causes memory leaks in very specific situations.
DistributedLockService.shutdown();
```

Note that not all methods are supported for all Lock types. For example, neither lock supports `newCondition()`, and
the reentrant lock does not support `lockInterruptibly()`, or untimed `tryLock()` due to limitations imposed by Hazelcast's
`ICondition.await()` method. These methods will throw UnsupportedOperationException.