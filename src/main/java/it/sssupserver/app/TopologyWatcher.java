package it.sssupserver.app;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Interact with erlang storage, share and get information
 * about seen topology
 */
public class TopologyWatcher {
    /**
     * Avoid suppling too old data, maintain
     * reference to last update time
     */
    private static class ValueContainer implements Comparable<ValueContainer> {
        private Instant timestamp;
        private DatanodeDescriptor value;

        public ValueContainer(DatanodeDescriptor value) {
            timestamp = Instant.now();
            this.value = value;
        }

        public Instant getTimestamp() {
            return timestamp;
        }

        public boolean isExpired(Instant currentTime, long expiration_ms) {
            return getTimestamp().plus(expiration_ms, ChronoUnit.MILLIS)
                .isBefore(currentTime);
        }

        public DatanodeDescriptor getValue() {
            return value;
        }

        @Override
        public int compareTo(ValueContainer arg0) {
            return timestamp.compareTo(arg0.timestamp);
        }

        @Override
        public boolean equals(Object other) {
            return other != null
                && other instanceof ValueContainer
                && ((ValueContainer)other).getTimestamp()
                    .equals(getTimestamp())
                && ((ValueContainer)other).getValue()
                    .equals(getValue());
        }
    }

    /**
     * Time beetween consecutive queries to the erlang
     * system.
     */
    private static final long ERLANG_DELAY = 5;

    /**
     * Consider local nodes expired if they
     * where not updated by EXPIRE_AFTER_MS
     */
    private static final long EXPIRE_AFTER_MS = 15*1000;

    // local view of the topology
    private ConcurrentMap<Long, ValueContainer> topology = new ConcurrentSkipListMap<>();

    // used to handle internal work
    private ExecutorService threadPool;
    private ScheduledExecutorService timedThreadPool;

    public TopologyWatcher(
        ExecutorService threadPool,
        ScheduledExecutorService timedThreadPool
    ) {
        if (threadPool == null || timedThreadPool == null) {
            throw new NullPointerException("Missing thread pools!");
        }
        this.threadPool = threadPool;
        this.timedThreadPool = timedThreadPool;
        this.hostname = App.getHostname();
    }

    /**
     * hostname, required to connect to erlang node
     */
    public String hostname;
    /**
     * Local subclass charged of interacting with Erlang
     * system,
     */
    private class ErlangSpeaker implements Runnable {

        private void handleExpiration() {
            try {
                var now = Instant.now();
                var expired = topology.values().stream()
                    .filter(vc -> vc.isExpired(now, EXPIRE_AFTER_MS))
                    .toArray(ValueContainer[]::new);
                for (var ex : expired) {
                    // inform erlang system
                    ;
                    // remove from local view
                    topology.remove(ex.getValue().getId(), ex);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            handleExpiration();
        }

    }
    private ErlangSpeaker erlangSpeaker = new ErlangSpeaker();
    private ScheduledFuture<?> schedule;

    /**
     * Connect to erlang topology
     */
    public void connectToErlang() {
        schedule = timedThreadPool.scheduleWithFixedDelay(
            erlangSpeaker, 0, ERLANG_DELAY, TimeUnit.SECONDS);
    }

    /**
     * Disconnect from erlang topology
     */
    public void disconnectFromErlang() {
        schedule.cancel(false);
    }

    public DatanodeDescriptor[] getRunningDatanodes() {
        var ans = topology.values().stream()
            .map(c -> c.getValue())
            .filter(d -> d.getStatus().equals("RUNNING"))
            .toArray(DatanodeDescriptor[]::new);
        return ans;
    }

    public DatanodeDescriptor[] getTopologySnapshot() {
        var ans = topology.values().stream()
            .map(c -> c.getValue())
            .toArray(DatanodeDescriptor[]::new);
        return ans;
    }

    /**
     * Add node, return snapshot
     */
    public DatanodeDescriptor[] notifyUpdate(DatanodeDescriptor node) {
        // insert only if status is valid
        if (node.isStatusValid()) {
            var v = new ValueContainer(node);
            topology.merge(node.getId(), v, (v1,v2) -> {
                if (v1.compareTo(v2) > 0) {
                    return v1;
                } else {
                    return v2;
                }
            });
        }
        return getTopologySnapshot();
    }
}
