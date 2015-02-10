package ca.thoughtwire.lock;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import org.junit.Ignore;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

@Ignore
public class HazelcastLockIT {

    public static void main(String[] args) {
        HazelcastLockIT instance = new HazelcastLockIT(args[0]);
        instance.test();

    }

    public HazelcastLockIT(String nodeNumber)
    {
        this.nodeNumber = nodeNumber;
    }

    public void test()
    {
        Config standardConfig;
        standardConfig = new ClasspathXmlConfig("hazelcast" + nodeNumber + ".xml");
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
                ReadWriteLock lock = lockService.getReentrantReadWriteLock(LOCK1);
                Lock writeLock = lock.writeLock();
                System.out.println("Counting down");
                latch.countDown();
                System.out.println("Waiting on latch");
                latch.await(20000, TimeUnit.MILLISECONDS);
                System.out.println("Trying writeLock...");
                if (writeLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                    try {
                        System.out.println("...writeLock acquired");
                        System.out.println("FAILURE.");
                        try {
                            // in lieu of doing something...
                            Thread.sleep(1000 * 2);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } finally {
                        writeLock.unlock();
                        System.out.println("writeLock released!");
                    }
                }
                else
                {
                    System.out.println("...try writeLock timed out.");
                    System.out.println("SUCCESS.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.exit(0);
        }
    }

    private String nodeNumber;
    private HazelcastInstance grid;
    private ICountDownLatch latch;

    private static final String LOCK1 = "Lock1";

}
