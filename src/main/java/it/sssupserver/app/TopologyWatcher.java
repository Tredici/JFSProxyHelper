package it.sssupserver.app;

import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpMbox;
import com.ericsson.otp.erlang.OtpNode;
import com.ericsson.otp.erlang.OtpNodeStatus;

/**
 * Interact with erlang storage, share and get information
 * about seen topology
 */
public class TopologyWatcher {
    /**
     * Avoid suppling too old data, maintain
     * reference to last update time
     */
    public static class ValueContainer implements Comparable<ValueContainer> {
        private Instant timestamp;
        private DatanodeDescriptor value;

        public ValueContainer(Instant timestamp, DatanodeDescriptor value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public ValueContainer(DatanodeDescriptor value) {
            this.timestamp = Instant.now();
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


    public static OtpErlangObject[] valueContainerToErlang(ValueContainer valueContainer) {
        if (valueContainer == null) {
            return null;
        }
        var descriptor = valueContainer.getValue();

        var ans = new OtpErlangObject[] {
            // keys
    /*[0]=*/new OtpErlangLong(descriptor.getId()),
    /*[1]=*/new OtpErlangLong(valueContainer.getTimestamp().toEpochMilli()),
            // values
    /*[2]=*/new OtpErlangLong(descriptor.getStartInstant().toEpochMilli()),
    /*[3]=*/new OtpErlangInt(descriptor.getReplicationFactor()),
    /*[4]=*/new OtpErlangString(descriptor.getStatus()),
    /*[5]=*/new OtpErlangList(
                Arrays.stream(descriptor.getDataEndpoints())
                    .map(URL::toString).map(OtpErlangString::new)
                    .toArray(OtpErlangObject[]::new)),
    /*[6]=*/new OtpErlangList(
                Arrays.stream(descriptor.getManagementEndpoint())
                    .map(URL::toString).map(OtpErlangString::new)
                    .toArray(OtpErlangObject[]::new)),
    /*[7]=*/new OtpErlangLong(descriptor.getLastStatusChange().toEpochMilli()),
    /*[8]=*/new OtpErlangLong(descriptor.getLastTopologyUpdate().toEpochMilli()),
    /*[9]=*/new OtpErlangLong(descriptor.getLastFileUpdate().toEpochMilli())
        };
        return ans;
    }

    public static ValueContainer erlangToValueContainer(OtpErlangObject[] eObj) {
        if (eObj == null || eObj.length != 10) {
            return null;
        }
        var descriptor = new DatanodeDescriptor();
        Instant timestamp;
        // key
/*[0]=*/descriptor.setId(((OtpErlangLong)eObj[0]).longValue());
/*[1]=*/timestamp = Instant.ofEpochMilli(((OtpErlangLong)eObj[1]).longValue());
        // values
/*[2]=*/descriptor.setStartInstant(Instant.ofEpochMilli(((OtpErlangLong)eObj[2]).longValue()));
/*[3]=*/descriptor.setReplicationFactor((int)((OtpErlangLong)eObj[3]).longValue());
/*[4]=*/descriptor.setStatus(((OtpErlangString)eObj[4]).stringValue());
        Function<String, URL> urlChecker = s -> {
            URL url;
            try {
                url = new URL(s); url.toURI();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Bad url: " + s, e);
            }
            if (url.getHost().length() == 0) {
                throw new RuntimeException("Bad url: " + s);
            }
            return url;
        };
/*[5]=*/descriptor.setDataEndpoints(
            Arrays.stream(
                ((OtpErlangList)eObj[5]).elements()
            ).map(o -> ((OtpErlangString)o).stringValue())
            .map(urlChecker)
            .toArray(URL[]::new));
/*[6]=*/descriptor.setDataEndpoints(
            Arrays.stream(
                ((OtpErlangList)eObj[6]).elements()
            ).map(o -> ((OtpErlangString)o).stringValue())
            .map(urlChecker)
            .toArray(URL[]::new));
/*[7]=*/descriptor.setLastStatusChange(Instant.ofEpochMilli(((OtpErlangLong)eObj[7]).longValue()));
/*[8]=*/descriptor.setLastTopologyUpdate(Instant.ofEpochMilli(((OtpErlangLong)eObj[8]).longValue()));
/*[9]=*/descriptor.setLastFileUpdate(Instant.ofEpochMilli(((OtpErlangLong)eObj[9]).longValue()));
        var ans = new ValueContainer(timestamp, descriptor);
        return ans;
    }

    /**
     * Time beetween consecutive queries to the erlang
     * system.
     */
    private static final long ERLANG_DELAY = 3;
    private static final long PING_DELAY_MS = 1000;

    /**
     * Consider local nodes expired if they
     * where not updated by EXPIRE_AFTER_MS
     */
    private static final long EXPIRE_AFTER_MS = 15*1000;

    // local view of the topology
    private ConcurrentMap<Long, ValueContainer> topology = new ConcurrentSkipListMap<>();

    private ValueContainer addToTopology(ValueContainer v) {
        var vc = topology.merge(v.getValue().getId(), v, (v1,v2) -> {
            if (v1.compareTo(v2) > 0) {
                return v1;
            } else {
                return v2;
            }
        });
        return vc;
    }

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

    private void handleExpiration() {
        try {
            var now = Instant.now();
            var expired = topology.values().stream()
                .filter(vc -> vc.isExpired(now, EXPIRE_AFTER_MS))
                .toArray(ValueContainer[]::new);
            for (var ex : expired) {
                // erlang automatically handle expiration interlang
                // remove from local view
                topology.remove(ex.getValue().getId(), ex);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // periodic task used to query erlang system
    private PeriodicTask periodicTask = new PeriodicTask();
    private class PeriodicTask implements Runnable {
        @Override
        public void run() {
            if (!isConnectedToErlang()) {
                tryToConnectToErlang();
            }
            if (isConnectedToErlang()) {
                tryToRequestSnapshot();
            }
            handleExpiration();
        }
    }

    // task run inside a thread used to handler incoming erlang messages
    private ReceiveTask receiveTask = new ReceiveTask();
    ScheduledFuture<?> receiveFuture;
    private void stopReceiveTask() {
        if (isConnectedToErlang()) {
            synchronized (receiveTask) {
                if (connected) {
                    connected = false;
                    // prevent race conditions
                    var f = receiveFuture;
                    if (f != null && !f.isCancelled()) {
                        f.cancel(false);
                    }
                }
            }
        }
    }
    private static final long ERLANG_POOL_INTERVALL = 300;
    private class ReceiveTask implements Runnable {
        @Override
        public void run() {
            if (connected == false) {
                // prevent race condition
                return;
            }
            OtpErlangObject msg = null;
            do {
                try {
                    msg = myMailBox.receive(0);
                    if (msg != null) {
                        System.out.println("Received erlang message: " + msg);
                        try {
                            if (msg instanceof OtpErlangTuple) {
                                var t = (OtpErlangTuple)msg;
                                if (t.arity() == 2) {
                                    if (t.elementAt(0).equals(snapshotRes)) {
                                        // snapshot reqsponse
                                        var snapshot = (OtpErlangList)t.elementAt(1);
                                        for (OtpErlangObject otpErlangObject : snapshot) {
                                            var vcTuple = (OtpErlangTuple)otpErlangObject;
                                            if (vcTuple.arity() != 10) {
                                                System.err.println("Invalid DatanodeDescriptor received");
                                            } else {
                                                var vc = erlangToValueContainer(vcTuple.elements());
                                                addToTopology(vc);
                                            }
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error occurred while erlang response");
                            e.printStackTrace();
                        }
                    }
                } catch (OtpErlangDecodeException e) {
                    e.printStackTrace();
                } catch (OtpErlangExit e) {
                    System.err.println("Disconnected");
                    stopReceiveTask();
                    connected = false;
                    break;
                }
            } while (msg != null);
        }
    }

    /**
     * Auxiliary class used to discover when remote node
     * goes down
     */
    private PeerWatcher peerWatcher = new PeerWatcher();
    private class PeerWatcher extends OtpNodeStatus {
        @Override
        public void remoteStatus(String node, boolean up, Object reason) {
            System.err.println("Remote status updated:\n"
                + "\tnode   => " + node + "\n"
                + "\tup     => " + up + "\n"
                + "\treason => " + reason);
            if (isConnectedToErlang()) {
                if (candidateNodes[currentPeer].equals(node) && !up) {
                    System.err.println("ATTENTION! Node " + node + " went down!");
                    stopReceiveTask();
                }
            }
        }
    }

    private ScheduledFuture<?> schedule;

    /**
     * Identifiers used to interact with the erlang subsistem
     */
    private String localNodeId;
    private String localMailBoxId;
    private String cookie;
    private String remoteProcessId;
    // try to connect to all available peers untill one success
    private int currentPeer = 0;
    private String[] candidateNodes;

    // erlang data structures
    private OtpNode thisNode;
    private OtpMbox myMailBox;

    private volatile boolean connected = false;

    private boolean isConnectedToErlang() {
        return connected;
    }

    private String getCurrentErlangPeer() {
        if (isConnectedToErlang()) {
            return candidateNodes[currentPeer];
        } else {
            return null;
        }
    }

    // attempt connecting to a remote erlang node
    private void tryToConnectToErlang() {
        if (!isConnectedToErlang()) {
            for (var i=0; i!=candidateNodes.length; ++i) {
                var test = candidateNodes[i];
                if (thisNode.ping(test, PING_DELAY_MS)) {
                    currentPeer = i;
                    connected = true;
                    // request snapshot
                    tryToRequestSnapshot();
                    // send all local seen topology
                    for (var vc : topology.values()) {
                        tryToNotifyErlang(vc);
                    }
                    // start mailbox polling
                    receiveFuture = timedThreadPool
                        .scheduleWithFixedDelay(receiveTask, 0, ERLANG_POOL_INTERVALL, TimeUnit.MILLISECONDS);
                    // should spawn linked node
                    // TODO: later
                    break;
                }
            }
            if (connected) {
                System.out.println("Successfully connected to erlang system!");
                System.out.println("Erlang node: " + getCurrentErlangPeer());
            } else {
                System.err.println("Failed to connect to erlang topology");
            }
        }
    }

    // attempt to query erlang system for DatanodeDescriptor[]
    private static final OtpErlangAtom snapshotReq = new OtpErlangAtom("snapshotReq");
    private static final OtpErlangAtom snapshotRes = new OtpErlangAtom("snapshotRes");
    private void tryToRequestSnapshot() {
        if (isConnectedToErlang()) {
            // send message {self(), getSnapshot}
            var msg = new OtpErlangTuple(
                new OtpErlangObject[] {
                    myMailBox.self(),
                    snapshotReq
                }
            );
            myMailBox.send(remoteProcessId, candidateNodes[currentPeer], msg);
        }
    }

    private static final OtpErlangAtom update = new OtpErlangAtom("update");
    private void tryToNotifyErlang(ValueContainer vc) {
        if (isConnectedToErlang()) {
            OtpErlangObject[] eval;
            try {
                eval = valueContainerToErlang(vc);
            } catch (Exception e) {
                System.err.println("Failed to 'erlangify' object");
                e.printStackTrace();
                return;
            }
            var content = new OtpErlangObject[]{
                myMailBox.self(),
                update,
                new OtpErlangTuple(eval)
            };
            var msg = new OtpErlangTuple(content);
            myMailBox.send(remoteProcessId, candidateNodes[currentPeer], msg);
        }
    }

    /**
     * Connect to erlang topology
     */
    public void connectToErlang(
            String localNodeId,
            String localMailBoxId,
            String cookie,
            String remoteProcessId,
            String[] candidateNodes
        ) throws IOException {
        this.localNodeId = localNodeId;
        this.localMailBoxId = localMailBoxId;
        this.cookie = cookie;
        this.remoteProcessId = remoteProcessId;
        this.candidateNodes = candidateNodes;
        if (thisNode != null) {
            myMailBox.close();
            thisNode.close();
            myMailBox = null;
            thisNode = null;
        }
        try {
            this.thisNode = new OtpNode(this.localNodeId, this.cookie);
            this.thisNode.registerStatusHandler(peerWatcher);
            this.myMailBox = thisNode.createMbox(this.localMailBoxId);
        } catch (IOException e) {
            System.err.println("Failed to initialize erlang OptNode");
            e.printStackTrace();
            throw e;
        }
        tryToConnectToErlang();

        schedule = timedThreadPool.scheduleWithFixedDelay(
            this.periodicTask, 0, ERLANG_DELAY, TimeUnit.SECONDS);
    }

    /**
     * Disconnect from erlang topology
     */
    public void disconnectFromErlang() {
        if (thisNode != null) {
            stopReceiveTask();
            myMailBox.close();
            thisNode.close();
            myMailBox = null;
            thisNode = null;
            schedule.cancel(false);
        }
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
            var vc = addToTopology(v);
            // try to notify erlang system
            tryToNotifyErlang(vc);
        }
        return getTopologySnapshot();
    }
}
