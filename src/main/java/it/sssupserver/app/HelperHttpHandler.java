package it.sssupserver.app;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;


public class HelperHttpHandler implements HttpHandler {

    public static final String PATH = "/";

    public static void httpPermanentRedirect(HttpExchange exchange, URL redirect) {
        try {
            // 308 Permanent Redirect
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections
            var path = exchange.getRequestURI().getPath().replaceAll(" ", "%20");
            exchange.getResponseHeaders()
                .add("Location", redirect.toURI()
                .resolve(path).toString());
            exchange.sendResponseHeaders(308, 0);
            exchange.getResponseBody().flush();
            // help .NET that is unable to cope with response-before-request
            if (exchange.getRequestMethod().equals("PUT")) {
                exchange.getRequestBody().transferTo(OutputStream.nullOutputStream());
            }
            exchange.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void httpBadRequest(HttpExchange exchange) {
        try {
            // 400 Bad Request
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/400
            var res = new StringBuilder()
                .append("400 Bad request")
                .append("\n")
                .toString()
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(400, res.length);
            exchange.getResponseBody().write(res);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void httpNotFound(HttpExchange exchange) {
        try {
            // 404 Not Found
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404
            var res = new StringBuilder()
                .append("404 Not Found ")
                .append(exchange.getRequestURI().toString())
                .append("\n")
                .toString()
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, res.length);
            exchange.getResponseBody().write(res);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void httpMethodNotAllowed(HttpExchange exchange) {
        try {
            // 405 Method Not Allowed
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405
            var res = new StringBuilder()
                .append("405 Method ")
                .append(exchange.getRequestMethod())
                .append(" not allowed.")
                .append("\n")
                .toString()
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(405, res.length);
            exchange.getResponseBody().write(res);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void httpInternalServerError(HttpExchange exchange) {
        try {
            // 500 Internal Server Error
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500
            var res = new StringBuilder()
                .append("500 Internal Server Error")
                .append("\n")
                .toString()
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, res.length);
            exchange.getResponseBody().write(res);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    public static void httpServiceUnavailable(HttpExchange exchange) {
        try {
            // 503 Service Unavailable
            //  https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/503
            var res = new StringBuilder()
                .append("503 Service Unavailable")
                .append("\n")
                .toString()
                .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, res.length);
            exchange.getResponseBody().write(res);
            exchange.getResponseBody().flush();
            exchange.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    private DatanodeDescriptorGson serializer;
    private TopologyWatcher watcher;
    public HelperHttpHandler(TopologyWatcher watcher) {
        this.serializer = new DatanodeDescriptorGson();
        this.watcher = watcher;
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        var bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        var os = exchange.getResponseBody();
        os.write(bytes);
        exchange.close();
    }

    // counters used to implement basic load balancing
    private AtomicInteger rn = new AtomicInteger(), ep = new AtomicInteger();
    /**
     * Find if request can be redirected to a real datanode
     */
    private void handleRedirect(HttpExchange exchange) {
        var running = watcher.getRunningDatanodes();
        if (running.length == 0) {
            // none available
            httpServiceUnavailable(exchange);
        } else {
            var l = running.length;
            var de = running[(int)(rn.getAndIncrement()%l)];
            var me = de.getManagementEndpoint();
            var lm = me.length;
            var url = me[(int)(ep.getAndIncrement()%lm)];
            httpPermanentRedirect(exchange, url);
        }
    }

    /**
     * Handle POST /api/topology
     */
    private void apiTopologyPOST(HttpExchange exchange) throws IOException {
        // get request body
        var data = exchange.getRequestBody().readAllBytes();
        var json = new String(data, StandardCharsets.UTF_8);
        DatanodeDescriptor receivedDD = null;
        // deserialze
        try {
            receivedDD = serializer.fromJson(json);
            if (receivedDD == null) {
                httpBadRequest(exchange);
                return;
            }
        } catch (Exception e) {
            httpBadRequest(exchange);
            return;
        }
        // notify watcher
        var topology = watcher.notifyUpdate(receivedDD);
        var res = serializer.toJson(topology);
        // send back current topology
        sendJson(exchange, res);
    }

    /**
     * Handle GET /api/topology
     */
    private void apiTopologyGet(HttpExchange exchange) throws IOException {
        var topology = watcher.getTopologySnapshot();
        var res = serializer.toJson(topology);
        // send back current topology
        sendJson(exchange, res);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (exchange.getRequestURI().getPath().startsWith("/file/")) {
                handleRedirect(exchange);
            } else if (exchange.getRequestMethod().equals("POST")) {
                if (exchange.getRequestURI().getPath().equals("/api/topology")) {
                    try {
                        apiTopologyPOST(exchange);
                    } catch (IOException e) {
                        // assert endpoint closure
                        exchange.close();
                    } catch (Exception e) {
                        httpInternalServerError(exchange);
                    }
                } else {
                    httpNotFound(exchange);
                }
            } else if (exchange.getRequestMethod().equals("GET")) {
                if (exchange.getRequestURI().getPath().equals("/api/topology")) {
                    try {
                        apiTopologyGet(exchange);
                    } catch (IOException e) {
                        // assert endpoint closure
                        exchange.close();
                    } catch (Exception e) {
                        httpInternalServerError(exchange);
                    }
                } else {
                    httpNotFound(exchange);
                }
            } else {
                httpMethodNotAllowed(exchange);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
