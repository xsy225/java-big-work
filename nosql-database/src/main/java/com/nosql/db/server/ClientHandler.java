package com.nosql.db.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.nosql.db.storage.DatabaseEngine;
import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;

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
        String clientAddress = clientSocket.getInetAddress().toString();
        logger.info("开始处理客户端连接: {}", clientAddress);

        try (BufferedReader in =
                new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                logger.debug("收到来自{}的命令: {}", clientAddress, inputLine);
                Command cmd = gson.fromJson(inputLine, Command.class);
                OperationResult result = executeCommand(cmd);
                out.println(gson.toJson(result));
                logger.debug("返回结果给{}: {}", clientAddress, result.isSuccess());

                if ("EXIT".equalsIgnoreCase(cmd.getCommand())) {
                    logger.info("客户端{}请求断开连接", clientAddress);
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("客户端{}通信异常: {}", clientAddress, e.getMessage());
        } finally {
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                    logger.info("客户端{}连接已关闭", clientAddress);
                }
            } catch (IOException e) {
                logger.warn("关闭客户端{}连接时发生异常: {}", clientAddress, e.getMessage());
            }
        }
    }

    private OperationResult executeCommand(Command cmd) {
        try {
            switch (cmd.getCommand()) {
                case "INSERT":
                    logger.debug("执行INSERT命令: 集合={}, ID={}", cmd.getCollection(),
                            cmd.getDocument().getId());
                    return databaseEngine.insertDocument(cmd.getCollection(), cmd.getDocument());
                case "UPDATE":
                    logger.debug("执行UPDATE命令: 集合={}, ID={}", cmd.getCollection(),
                            cmd.getDocument().getId());
                    return databaseEngine.updateDocument(cmd.getCollection(), cmd.getDocument());
                case "DELETE":
                    logger.debug("执行DELETE命令: 集合={}, ID={}", cmd.getCollection(), cmd.getId());
                    return databaseEngine.deleteDocument(cmd.getCollection(), cmd.getId());
                case "GET":
                    logger.debug("执行GET命令: 集合={}, ID={}", cmd.getCollection(), cmd.getId());
                    return databaseEngine.getDocument(cmd.getCollection(), cmd.getId());
                case "GET_ALL":
                    logger.debug("执行GET_ALL命令: 集合={}", cmd.getCollection());
                    return databaseEngine.getAllDocuments(cmd.getCollection());
                case "EXIT":
                    return new OperationResult(true, "连接关闭");
                default:
                    logger.warn("未知命令: {}", cmd.getCommand());
                    return new OperationResult(false, "未知命令: " + cmd.getCommand());
            }
        } catch (Exception e) {
            logger.error("命令执行失败: {}", e.getMessage(), e);
            return new OperationResult(false, "错误: " + e.getMessage());
        }
    }

    static class Command {
        private String command;
        private String collection;
        private String id;
        private Document document;

        public String getCommand() {
            return command;
        }

        public void setCommand(String command) {
            this.command = command;
        }

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Document getDocument() {
            return document;
        }

        public void setDocument(Document document) {
            this.document = document;
        }
    }
}
