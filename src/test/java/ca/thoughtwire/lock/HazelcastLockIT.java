package ca.thoughtwire.lock;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * @author vanessa.williams
 */
public class HazelcastLockIT {

    public static void main(String[] args)
    {
        /*
         * 1st arg = 1 if I'm the process that's started 1st & will crash; 0 if not
         */

        HazelcastLockIT instance = new HazelcastLockIT(args[0].equals("1"));
        instance.test();
    }

    public HazelcastLockIT(boolean crashProcess)
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
                Lock lock1, lock2, lock3, lock4;
                lock1 = lockService.getReentrantReadWriteLock(readLock1).readLock();
                lock2 = lockService.getReentrantReadWriteLock(readLock2).readLock();
                lock3 = lockService.getReentrantReadWriteLock(writeLock1).writeLock();
                lock4 = lockService.getReentrantReadWriteLock(writeLock2).writeLock();
                if (crashProcess)
                {
                    System.out.println("Crash process acquiring locks");
                    lock1.lock();
                    lock2.lock();
                    lock3.lock();
                    lock4.lock();
                    System.out.println("Crash process counting down...");
                    latch.countDown();
                    System.out.println("Locks acquired");
                    System.out.println("Waiting on latch...");
                    latch.await(20000, TimeUnit.MILLISECONDS);
                    System.out.println("Exiting normally");
                    System.exit(0);
                }
                else
                {
                    System.out.println("Counting down...");
                    latch.countDown();
                    System.out.println("Waiting on latch...");
                    latch.await(20000, TimeUnit.MILLISECONDS);
                    System.out.println("Acquiring locks...");
                    lock1.lock();
                    lock2.lock();
                    lock3.lock();
                    lock4.lock();
                    System.out.println("Locks acquired");
                    lock1.unlock();
                    lock2.unlock();
                    lock3.unlock();
                    lock4.unlock();
                    System.out.println("Locks released");
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

    private static final String readLock1 = "readLock1";
    private static final String readLock2 = "readLock2";
    private static final String writeLock1 = "writeLock1";
    private static final String writeLock2 = "writeLock2";
}

