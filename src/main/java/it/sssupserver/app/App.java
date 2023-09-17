package it.sssupserver.app;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;

/**
 * This project expose two HTTP Endpoint:
 *  POST /api/topology, to simplify CDN DataNode(s)
 *  discovery.
 *  The response consist of a single item containing
 *  an array field "Topology" listing the current topology.
 *
 *  GET /apy/topoly
 *  same result, used to inspect currently seen topology
 *
 * A single value is expected to be supplied from command line:
 *  listening endpoint, presented as an http URL with no path
 * If no argument is supplied, default is used:
 *  http://0.0.0.0:8008/
 */
public class App 
{
    public static final String DEFAULT_CONFIG = "DispatcherConfig.json";
    public static final int BACKLOG = 128;

    // thread pools used to handle client requests
    private static ExecutorService threadPool;
    private static ScheduledExecutorService timedThreadPool;

    private static String hostname;
    public static String getHostnameOrThrow() throws UnknownHostException {
        if (hostname == null) {
            hostname = InetAddress.getLocalHost().getHostName();
        }
        return hostname;
    }

    public static String getHostname() {
        try {
            return getHostnameOrThrow();
        } catch (UnknownHostException e) {
            System.err.println("Failed to obtain hostname! Print stacktrace and abort.");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * This project is expected to handle very few requests
     * per seconds, so a single thread per pool is enough
     */
    private static void initThreadPools() {
        threadPool = Executors.newSingleThreadExecutor();
        timedThreadPool = Executors.newSingleThreadScheduledExecutor();
    }

    private static URL getListeningEndpoint() {
        var ans = config.getListeningEndpoint();
        if (!ans.getProtocol().isEmpty() && !ans.getProtocol().equals("http")) {
            System.err.println("Error: only http is currently supported!");
            System.exit(1);
        }
        return ans;
    }

    private static URL url;

    /**
     * Extract InetSocketAddress from URL
     */
    private static InetSocketAddress getListeningInetSocketAddress() {
        var port = url.getPort();
        var address = url.getHost();
        // should be http or nothing
        var ans = new InetSocketAddress(address, port);
        return ans;
    }

    private static TopologyWatcher topologyWatcher;

    // server
    private static HttpServer httpServer;
    /**
     * Start
     * @throws IOException
     */
    private static void startServer() throws IOException {
        // extract InetSocketAddress
        var isa = getListeningInetSocketAddress();
        // activate PeerWatcher
        try {
            System.out.println("Starting server on: " + isa);
            httpServer = HttpServer.create(isa, BACKLOG);
            System.out.println("Socket opened!");
        } catch (IOException e) {
            System.err.println("Failed to connect httpserver to endpoint: " + isa.toString());
            throw e;
        }
        // init thread pools
        initThreadPools();
        // supply thread pool to http server
        httpServer.setExecutor(threadPool);
        // launch erlang-speaker
        topologyWatcher = new TopologyWatcher(threadPool, timedThreadPool);
        System.out.println("Starting erlang subsistem...");
        try {
            topologyWatcher.connectToErlang(
                config.getLocalNodeId(),
                config.getLocalMailBoxId(),
                config.getCookie(),
                config.getRemoteProcessId(),
                config.getCandidateNodes()
            );
        } catch (IOException e) {
            System.err.println("Failed to connect to erlang");
            throw e;
        }
        System.out.println("Started!");
        var handler = new HelperHttpHandler(topologyWatcher);
        httpServer.createContext(HelperHttpHandler.PATH, handler);
        // connect to erlang
        httpServer.start();
        System.out.println("HTTP server listing on " + isa);
    }

    private static void waitForTerminationRequest() {
        var exit = false;
        try(var in = new Scanner(System.in)) {
            do {
                System.out.print("Insert 'stop' to exit> ");
                var line = in.nextLine();
                if (line.equals("stop")) {
                    System.out.print("Confirm [y/N]? ");
                    var confirm = in.nextLine();
                    if (confirm.equals("y")) {
                        exit = true;
                    }
                }
            } while (!exit);
        }
    }


    private static void stopServer() throws InterruptedException {
        System.out.println("Stopping web server...");
        var stopDelay = 5;
        httpServer.stop(stopDelay);
        System.out.println("Stopped!");
        System.out.println("Stopping erlang subsystem...");
        topologyWatcher.disconnectFromErlang();
        System.out.println("Stopped!");
        threadPool.shutdown();
        timedThreadPool.shutdown();
        threadPool.awaitTermination(5, TimeUnit.SECONDS);
        timedThreadPool.awaitTermination(5, TimeUnit.SECONDS);
    }

    private static DispatcherConfiguration config;

    private static void readConfiguration(String[] args) throws FileNotFoundException, IOException {
        if (args.length > 1) {
            System.err.println("Invalid arguments! At most one argument is accepted");
            System.exit(1);
        }
        var configFile = args.length == 0 ? DEFAULT_CONFIG : args[0];
        config = DispatcherConfiguration.fromFile(configFile);
    }

    public static void main(String[] args) throws InterruptedException
    {
        try {
            readConfiguration(args);
            url = getListeningEndpoint();
            startServer();
            waitForTerminationRequest();
            stopServer();
        } catch (Exception e) {
            System.err.println("CRASHED!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
