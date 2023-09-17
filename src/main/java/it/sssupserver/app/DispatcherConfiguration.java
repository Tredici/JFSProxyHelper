package it.sssupserver.app;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * 
 */
public class DispatcherConfiguration
{
    private static ConfigurationGson serializer = new ConfigurationGson();

    private URL listeningEndpoint;
    private String localNodeId;
    private String localMailBoxId;
    private String cookie;
    private String remoteProcessId;
    private String[] candidateNodes;

    public URL getListeningEndpoint() {
        return listeningEndpoint;
    }
    public String getLocalNodeId() {
        return localNodeId;
    }
    public String getLocalMailBoxId() {
        return localMailBoxId;
    }
    public String getCookie() {
        return cookie;
    }
    public String getRemoteProcessId() {
        return remoteProcessId;
    }
    public String[] getCandidateNodes() {
        return candidateNodes;
    }

    //Dispatcher
    public static class ConfigurationGson
        implements JsonDeserializer<DispatcherConfiguration>,
        JsonSerializer<DispatcherConfiguration> {
    
        private static Gson gson;
        public ConfigurationGson() {
            gson = new GsonBuilder()
                .registerTypeAdapter(DispatcherConfiguration.class, this)
                .create();
        }
    
        @Override
        public JsonElement serialize(DispatcherConfiguration src, Type typeOfSrc, JsonSerializationContext context) {
            var jObj = new JsonObject();
            jObj.addProperty("ListeningEndpoint", src.getListeningEndpoint().toString());
            jObj.addProperty("LocalNodeId", src.getLocalNodeId());
            jObj.addProperty("LocalMailBoxId", src.getLocalMailBoxId());
            jObj.addProperty("Cookie", src.getCookie());
            jObj.addProperty("RemoteProcessId", src.getRemoteProcessId());          
            {
                var nodes = src.getCandidateNodes();
                var jArray = new JsonArray(nodes.length);
                for (var node : nodes) {
                    jArray.add(node);
                }
                jObj.add("CandidateNodes", jArray);
            }
            return jObj;
        }
    
        @Override
        public DispatcherConfiguration deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            var ans = new DispatcherConfiguration();
            var jObj = json.getAsJsonObject();
            try {
                ans.listeningEndpoint = new URL(jObj.get("ListeningEndpoint").getAsString());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to parse Config.ListeningEndpoint", e);
            }
            ans.localNodeId = jObj.get("LocalNodeId").getAsString();
            ans.localMailBoxId = jObj.get("LocalMailBoxId").getAsString();
            ans.cookie = jObj.get("Cookie").getAsString();
            ans.remoteProcessId = jObj.get("RemoteProcessId").getAsString();
            {
                var jArray = jObj.getAsJsonArray("CandidateNodes");
                var nodes = new String[jArray.size()];
                var i = 0;
                for (var item : jArray.asList()) {
                    nodes[i++] = item.getAsString();
                }
                ans.candidateNodes = nodes;
            }
            return ans;
        }
    
        public DispatcherConfiguration fromJson(String json) {
            DispatcherConfiguration ans;
            ans = gson.fromJson(json, DispatcherConfiguration.class);
            return ans;
        }
    
        public DispatcherConfiguration fromJson(FileReader fr) {
            DispatcherConfiguration ans;
            ans = gson.fromJson(fr, DispatcherConfiguration.class);
            return ans;
        }
    }

    public static DispatcherConfiguration fromFile(String path) throws FileNotFoundException, IOException {
        DispatcherConfiguration ans;
        try (var fr = new FileReader(path)) {
            ans = serializer.fromJson(fr);
        }
        return ans;
    }
}

