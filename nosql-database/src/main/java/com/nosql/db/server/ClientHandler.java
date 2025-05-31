package com.nosql.db.server;

import com.google.gson.Gson;
import com.nosql.db.storage.DatabaseEngine;
import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);
    private final Socket clientSocket;
    private final DatabaseEngine databaseEngine;
    private final Gson gson = new Gson();

    public ClientHandler(Socket clientSocket, DatabaseEngine databaseEngine) {
        this.clientSocket = clientSocket;
        this.databaseEngine = databaseEngine;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
            
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                logger.debug("收到命令: {}", inputLine);
                Command cmd = gson.fromJson(inputLine, Command.class);
                OperationResult result = executeCommand(cmd);
                out.println(gson.toJson(result)); // 返回JSON结果
                
                if ("EXIT".equalsIgnoreCase(cmd.getCommand())) break;
            }
        } catch (IOException e) {
            logger.error("客户端处理错误: ", e);
        }
    }

    private OperationResult executeCommand(Command cmd) {
        try {
            switch (cmd.getCommand()) {
                case "INSERT":
                    return databaseEngine.insertDocument(cmd.getCollection(), cmd.getDocument());
                case "UPDATE":
                    return databaseEngine.updateDocument(cmd.getCollection(), cmd.getDocument());
                case "DELETE":
                    return databaseEngine.deleteDocument(cmd.getCollection(), cmd.getId());
                case "GET":
                    return databaseEngine.getDocument(cmd.getCollection(), cmd.getId()); // 调用新方法
                case "GET_ALL":
                    return databaseEngine.getAllDocuments(cmd.getCollection()); // 调用新方法
                case "EXIT":
                    return new OperationResult(true, "连接关闭");
                default:
                    return new OperationResult(false, "未知命令: " + cmd.getCommand());
            }
        } catch (Exception e) {
            logger.error("命令执行错误: ", e);
            return new OperationResult(false, "错误: " + e.getMessage());
        }
    }

    // 内部类：定义命令格式
    static class Command {
        private String command;
        private String collection;
        private String id;
        private Document document;

        // Getters and Setters
        public String getCommand() { return command; }
        public void setCommand(String command) { this.command = command; }
        public String getCollection() { return collection; }
        public void setCollection(String collection) { this.collection = collection; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public Document getDocument() { return document; }
        public void setDocument(Document document) { this.document = document; }
    }
}