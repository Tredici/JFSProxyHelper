package it.sssupserver.app;

import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;


public class DatanodeDescriptor {

    // used to assert correctness of received status
    public static final Set<String> validStates = Collections.unmodifiableSet(
        Set.of("SHUTDOWN", "STARTING", "SYNCING",
            "RUNNING", "STOPPING", "MAYBE_FAILED", "FAILED")
    );

    // used to check
    public boolean isStatusValid() {
        return validStates.contains(getStatus());
    }

    private long id;                // Node Id
    private Instant startInstant;   // .
    private int replicationFactor;  // replicas requested by the node
    private String status;          // status of the node

    private URL[] dataEndpoints;        // data endpoints
    private URL[] managementEndpoint;   // management endpoints, to be spread

    private Instant lastStatusChange;   // last time node changed its status
    private Instant lastTopologyUpdate; // last time node chenged its topology vies
    private Instant lastFileUpdate;     // node updated its local storage

    public long getId() { return id; }
    public void setId(long id) { this.id = id; }

    public Instant getStartInstant() { return startInstant; }
    public void setStartInstant(Instant startInstant) { this.startInstant = startInstant; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public URL[] getDataEndpoints() { return dataEndpoints; }
    public URL[] getManagementEndpoint() { return managementEndpoint; }
    public void setDataEndpoints(URL[] dataEndpoints) { this.dataEndpoints = dataEndpoints; }
    public void setManagementEndpoint(URL[] managementEndpoint) { this.managementEndpoint = managementEndpoint; }

    public Instant getLastStatusChange() { return lastStatusChange; }
    public void setLastStatusChange(Instant lastStatusChange) { this.lastStatusChange = lastStatusChange; }

    public Instant getLastTopologyUpdate() { return lastTopologyUpdate; }
    public void setLastTopologyUpdate(Instant lastTopologyUpdate) { this.lastTopologyUpdate = lastTopologyUpdate; }

    public Instant getLastFileUpdate() { return lastFileUpdate; }
    public void setLastFileUpdate(Instant lastFileUpdate) { this.lastFileUpdate = lastFileUpdate; }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if(other instanceof DatanodeDescriptor) {
            var o = (DatanodeDescriptor)other;
            // compare all members
            return id == o.id
                && startInstant.equals(o.startInstant)
                && replicationFactor == o.replicationFactor
                && status.equals(o.status)
                && Arrays.equals(dataEndpoints, o.dataEndpoints)
                && Arrays.equals(managementEndpoint, o.managementEndpoint)
                && lastStatusChange.equals(o.lastStatusChange)
                && lastTopologyUpdate.equals(o.lastTopologyUpdate)
                && lastFileUpdate.equals(o.lastFileUpdate);
        } else {
            return false;
        }
    }
}
