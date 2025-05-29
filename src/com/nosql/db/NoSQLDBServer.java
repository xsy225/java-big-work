package com.nosql.db;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NoSQLDBServer {
    private static final int DEFAULT_PORT = 8080;
    private final int port;
    private final ExecutorService threadPool;
    private final StorageEngine storageEngine;
    private final LogManager logManager;
    private final CacheManager cacheManager;
    private final IndexManager indexManager;
    private ServerSocket serverSocket;
    private boolean running;
    private final ClusterManager clusterManager;
    private final Map<String, Database> databases; // 数据库集合（线程安全）

    public NoSQLDBServer(int port) {
        this.port = port;
        this.threadPool = Executors.newFixedThreadPool(100); // 线程池处理多客户端
        this.storageEngine = new StorageEngine();
        this.logManager = new LogManager();
        this.cacheManager = new CacheManager();
        this.indexManager = new IndexManager();
        this.clusterManager = new ClusterManager(this);
        this.databases = new ConcurrentHashMap<>(); // 使用线程安全的Map
    }

    /**
     * 启动服务器
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        System.out.println("NoSQLDB Server started on port " + port);

        // 初始化：加载日志并恢复数据
        logManager.recover();
        
        // 启动集群管理
        clusterManager.start();

        // 循环接收客户端连接
        while (running) {
            Socket clientSocket = serverSocket.accept();
            threadPool.submit(new ClientHandler(clientSocket, this)); // 每个客户端分配独立线程处理
        }
    }

    /**
     * 停止服务器
     */
    public void stop() {
        running = false;
        clusterManager.stop();
        threadPool.shutdown();
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            // 关闭资源
            logManager.close();
            storageEngine.shutdown();
        } catch (IOException e) {
            System.err.println("Server shutdown error: " + e.getMessage());
        }
    }

    /**
     * 获取数据库实例（不存在则创建）
     */
    public Database getDatabase(String dbName) {
        return databases.computeIfAbsent(dbName, Database::new);
    }

    /**
     * 主函数入口
     */
    public static void main(String[] args) {
        try {
            NoSQLDBServer server = new NoSQLDBServer(DEFAULT_PORT);
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}