package it.sssupserver.app;

import java.lang.reflect.Type;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class DatanodeDescriptorGson
    implements JsonSerializer<DatanodeDescriptor>,
    JsonDeserializer<DatanodeDescriptor> {

    @Override
    public DatanodeDescriptor deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        var jObj = json.getAsJsonObject();
        var ans = new DatanodeDescriptor();
        // read default properties
        ans.setId(jObj.get("Id").getAsLong());
        ans.setReplicationFactor(jObj.get("ReplicationFactor").getAsInt());
        ans.setStatus(jObj.get("Status").getAsString());
        ans.setStartInstant(Instant.ofEpochMilli(jObj.get("StartInstant").getAsLong()));
        ans.setLastFileUpdate(Instant.ofEpochMilli(jObj.get("LastFileUpdate").getAsLong()));
        ans.setLastTopologyUpdate(Instant.ofEpochMilli(jObj.get("LastTopologyUpdate").getAsLong()));
        ans.setLastStatusChange(Instant.ofEpochMilli(jObj.get("LastStatusChange").getAsLong()));
        {
            // read management endpoints
            var jArray = jObj.get("ManagerEndpoint").getAsJsonArray();
            var mes = (URL[])context.deserialize(jArray, URL[].class);
            ans.setManagementEndpoint(mes);
        }
        {
            // read management endpoints
            var jArray = jObj.get("DataEndpoints").getAsJsonArray();
            var des = (URL[])context.deserialize(jArray, URL[].class);
            ans.setDataEndpoints(des);
        }
        return ans;
    }

    @Override
    public JsonElement serialize(DatanodeDescriptor src, Type typeOfSrc, JsonSerializationContext context) {
        var jObj = new JsonObject();
        jObj.addProperty("Id", src.getId());
        jObj.addProperty("ReplicationFactor", src.getReplicationFactor());
        jObj.add("DataEndpoints", context.serialize(src.getDataEndpoints()));
        jObj.add("ManagerEndpoint", context.serialize(src.getManagementEndpoint()));
        jObj.addProperty("StartInstant", src.getStartInstant().toEpochMilli());
        jObj.addProperty("Status", src.getStatus());
        jObj.addProperty("LastFileUpdate", src.getLastFileUpdate().toEpochMilli());
        jObj.addProperty("LastStatusChange", src.getLastStatusChange().toEpochMilli());
        jObj.addProperty("LastTopologyUpdate", src.getLastTopologyUpdate().toEpochMilli());
        return jObj;
    }

    private Gson gson;
    public DatanodeDescriptorGson() {
        this.gson = new GsonBuilder()
            .registerTypeAdapter(DatanodeDescriptor.class, this)
            .create();
    }

    /**
     * Deserialize a single object, used to parse
     * POST /api/topology request payload
     */
    public DatanodeDescriptor fromJson(String json) {
        DatanodeDescriptor ans;
        ans = gson.fromJson(json, DatanodeDescriptor.class);
        return ans;
    }

    /**
     * Serialize
     * POST /api/topology response payload
     */
    public String toJson(DatanodeDescriptor[] topology) {
        var obj = Collections.singletonMap("Topology", topology);
        var ans = gson.toJson(obj);
        return ans;
    }
}
