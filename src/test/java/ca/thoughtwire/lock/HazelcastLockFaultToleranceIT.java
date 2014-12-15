package ca.thoughtwire.lock;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author vanessa.williams
 */
public class HazelcastLockFaultToleranceIT {

    public static void main(String[] args)
    {
        /*
         * 1st arg = 1 if I'm the process that's started 1st & will crash; 0 if not
         */

        HazelcastLockFaultToleranceIT instance = new HazelcastLockFaultToleranceIT(args[0].equals("1"));
        instance.test();
    }

    public HazelcastLockFaultToleranceIT(boolean crashProcess)
    {
        this.crashProcess = crashProcess;
    }

    public void test()
    {
        Config standardConfig;
        if (crashProcess)
        {
            standardConfig = new ClasspathXmlConfig("hazelcast1.xml");
        }
        else
        {
            standardConfig = new ClasspathXmlConfig("hazelcast2.xml");
        }
        standardConfig.setProperty("hazelcast.operation.call.timeout.millis", "3000");
        grid = Hazelcast.newHazelcastInstance(standardConfig);
        latch = grid.getCountDownLatch("crash_lock");
        latch.trySetCount(2);
        Locker locker = new Locker();
        new Thread(locker).run();
    }

    class Locker implements Runnable
    {
        @Override
        public void run() {
            DistributedLockService lockService = DistributedLockService.newHazelcastLockService(grid);
            try {
                ReadWriteLock lock1, lock2, lock3, lock4;
                lock1 = lockService.getReentrantReadWriteLock(LOCK1);
                lock2 = lockService.getReentrantReadWriteLock(LOCK2);
                lock3 = lockService.getReentrantReadWriteLock(LOCK3);
                lock4 = lockService.getReentrantReadWriteLock(LOCK4);
                if (crashProcess)
                {
                    System.out.println("Crash process acquiring locks");
                    lock1.readLock().lock();
                    lock2.readLock().lock();
                    lock3.writeLock().lock();
                    lock4.writeLock().lock();
                    System.out.println("Crash process counting down...");
                    latch.countDown();
                    System.out.println("Locks acquired");
                    System.out.println("Waiting on latch...");
                    latch.await(20000, TimeUnit.MILLISECONDS);
                    System.out.println("Exiting");
                    System.exit(1);
                }
                else
                {
                    System.out.println("Counting down...");
                    latch.countDown();
                    System.out.println("Waiting on latch...");
                    latch.await(20000, TimeUnit.MILLISECONDS);
                    System.out.println("Latch wait finished, acquiring locks on crashed read locks...");
                    lock1.readLock().lock();
                    lock2.readLock().lock();
                    System.out.println("Read locks acquired, acquiring locks on crashed write locks...");
                    lock3.readLock().lock();
                    lock4.writeLock().lock();
                    System.out.println("Locks acquired");
                    lock1.readLock().unlock();
                    lock2.readLock().unlock();
                    lock3.readLock().unlock();
                    lock4.writeLock().unlock();
                    System.out.println("Locks released");
                    lockService.shutdown();
                    grid.getLifecycleService().shutdown();
                    System.exit(0);
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }
    private boolean crashProcess = false;
    private HazelcastInstance grid;
    private ICountDownLatch latch;

    private static final String LOCK1 = "Lock1";
    private static final String LOCK2 = "Lock2";
    private static final String LOCK3 = "Lock3";
    private static final String LOCK4 = "Lock4";
}

