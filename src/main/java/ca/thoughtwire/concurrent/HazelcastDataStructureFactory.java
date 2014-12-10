package ca.thoughtwire.concurrent;

import com.hazelcast.core.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Factory for creating Hazelcast data structures.
 *
 * @author vanessa.williams
 */
public class HazelcastDataStructureFactory implements DistributedDataStructureFactory, MembershipListener {

    public static HazelcastDataStructureFactory getInstance(HazelcastInstance hazelcastInstance)
    {
        HazelcastDataStructureFactory factory = new HazelcastDataStructureFactory(hazelcastInstance);
        hazelcastInstance.getCluster().addMembershipListener(factory);
        return factory;
    }

    /**
     * The constructor is private because a static factory method is required in order to
     * prevent the "this" reference from escaping during construction (see Java Concurrency
     * in Practice, Section 3.2.1 Safe construction practices)
     *
     * @param hazelcastInstance
     */
    private HazelcastDataStructureFactory(HazelcastInstance hazelcastInstance)
    {
        this.hazelcastInstance = hazelcastInstance;
        this.nodeId = hazelcastInstance.getCluster().getLocalMember().getUuid();
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
    public <K, V> DistributedMultiMap<K, V> getMultiMap(String name) {
        return new HazelcastMultiMap<K, V>(hazelcastInstance, name);
    }

    @Override
    public Lock getLock(String name) {
        return hazelcastInstance.getLock(name);
    }

    @Override
    public Condition getCondition(Lock lock, String conditionName) {
        return ((ILock)lock).newCondition(conditionName);
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void addMembershipListener(GridMembershipListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeMembershipListener(GridMembershipListener listener) {
        listeners.remove(listener);
    }

    @Override
    public Collection<GridMembershipListener> getListeners() {
        return Collections.unmodifiableCollection(listeners);
    }

    @Override
    public void memberAdded(MembershipEvent membershipEvent) {
        final String uuid = membershipEvent.getMember().getUuid();
        for (GridMembershipListener listener: getListeners())
        {
            listener.memberAdded(uuid);
        }
    }

    @Override
    public void memberRemoved(MembershipEvent membershipEvent) {
        final String uuid = membershipEvent.getMember().getUuid();
        for (GridMembershipListener listener: getListeners())
        {
            listener.memberRemoved(uuid);
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        // NOOP
    }

    protected final HazelcastInstance hazelcastInstance;

    protected final String nodeId;
    /* Membership event listeners */
    protected final List<GridMembershipListener> listeners =
            Collections.synchronizedList(new ArrayList<GridMembershipListener>());

}
