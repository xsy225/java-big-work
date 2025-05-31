package com.nosql.db.server;

import com.google.gson.Gson;
import com.nosql.db.storage.DatabaseEngine;
import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;
import java.io.BufferedReader;  // 添加导入
import java.io.IOException;
import java.io.InputStreamReader;  // 添加导入
import java.io.PrintWriter;  // 添加导入
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NoSQLServer {
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
            System.out.println("服务器启动，监听端口: " + port);
            databaseEngine.recoverFromWal();
            
            while (running) {
                Socket clientSocket = serverSocket.accept();
                threadPool.execute(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("服务器启动失败: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
            
            String request = in.readLine();
            if (request == null) return;
            
            OperationResult result = processRequest(request);
            out.println(new Gson().toJson(result));
            
        } catch (IOException e) {
            System.err.println("客户端处理错误: " + e.getMessage());
        }
    }

    private OperationResult processRequest(String request) {
        try {
            String[] parts = request.split(" ", 4);
            if (parts.length < 3) return new OperationResult(false, "无效请求格式");
            
            String cmd = parts[0].toUpperCase();
            String collection = parts[1];
            String docId = parts[2];
            String data = parts.length > 3 ? parts[3] : null;
            
            switch (cmd) {
                case "INSERT":
                    if (data == null) return new OperationResult(false, "缺少文档数据");
                    return databaseEngine.insertDocument(collection, Document.fromJson(data));
                    
                case "UPDATE":
                    if (data == null) return new OperationResult(false, "缺少文档数据");
                    return databaseEngine.updateDocument(collection, Document.fromJson(data));
                    
                case "DELETE":
                    return databaseEngine.deleteDocument(collection, docId);
                    
                case "GET":
                    return databaseEngine.getDocument(collection, docId);
                    
                case "GET_ALL":
                    return databaseEngine.getAllDocuments(collection);
                    
                default:
                    return new OperationResult(false, "未知命令: " + cmd);
            }
        } catch (Exception e) {
            return new OperationResult(false, "请求处理失败: " + e.getMessage());
        }
    }

    public void shutdown() {
        running = false;
        threadPool.shutdown();
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) { /* 忽略 */ }
        System.out.println("服务器已关闭");
    }
}