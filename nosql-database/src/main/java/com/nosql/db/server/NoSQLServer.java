package com.nosql.db.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nosql.db.storage.DatabaseEngine;

public class NoSQLServer {
    private static final Logger logger = LoggerFactory.getLogger(NoSQLServer.class);
    private final int port;
    private final ExecutorService threadPool;
    private final DatabaseEngine databaseEngine;
    private ServerSocket serverSocket;
    private volatile boolean running = false;

    public NoSQLServer(int port, int threadPoolSize, DatabaseEngine databaseEngine) {
        this.port = port;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize);
        this.databaseEngine = databaseEngine;
    }

    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            running = true;
            logger.info("服务器启动，监听端口: {}", port);
            databaseEngine.recoverFromWal();
            logger.info("完成WAL日志恢复");

            while (running) {
                Socket clientSocket = serverSocket.accept();
                logger.info("新客户端连接: {}", clientSocket.getInetAddress());
                threadPool.execute(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            if (running) { // 忽略关闭时的异常
                logger.error("服务器启动失败: {}", e.getMessage());
            }
        } finally {
            shutdown();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (clientSocket) {
            logger.debug("开始处理客户端请求: {}", clientSocket.getInetAddress());
            ClientHandler handler = new ClientHandler(clientSocket, databaseEngine);
            handler.run();
            logger.debug("完成处理客户端请求: {}", clientSocket.getInetAddress());
        } catch (Exception e) {
            logger.error("客户端处理异常: {}", e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        threadPool.shutdown();
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                logger.info("服务器套接字已关闭");
            }
        } catch (IOException e) {
            logger.warn("关闭服务器套接字时发生异常: {}", e.getMessage());
        }
        logger.info("服务器已关闭");
    }
}
