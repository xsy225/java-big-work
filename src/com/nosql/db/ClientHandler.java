package com.nosql.db;

import java.io.*;
import java.net.Socket;

// 客户端处理器 - 处理客户端请求
class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private final NoSQLDBServer server;
    private ObjectInputStream input;
    private ObjectOutputStream output;
    
    public ClientHandler(Socket clientSocket, NoSQLDBServer server) {
        this.clientSocket = clientSocket;
        this.server = server;
    }
    
    @Override
    public void run() {
        try {
            output = new ObjectOutputStream(clientSocket.getOutputStream());
            input = new ObjectInputStream(clientSocket.getInputStream());
            
            // 处理客户端请求
            Request request;
            while ((request = (Request) input.readObject()) != null) {
                Response response = processRequest(request);
                output.writeObject(response);
                output.flush();
            }
        } catch (IOException | ClassNotFoundException e) {
            // 处理异常
        } finally {
            try {
                if (input != null) input.close();
                if (output != null) output.close();
                if (clientSocket != null && !clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    private Response processRequest(Request request) {
        try {
            switch (request.getCommand()) {
                case "GET":
                    return handleGet(request);
                case "PUT":
                    return handlePut(request);
                case "DELETE":
                    return handleDelete(request);
                case "CREATE_DB":
                    return handleCreateDB(request);
                case "CREATE_COLLECTION":
                    return handleCreateCollection(request);
                // 处理其他命令...
                default:
                    return new Response(false, "未知命令: " + request.getCommand());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new Response(false, "处理请求时出错: " + e.getMessage());
        }
    }
    
    // 处理各种请求的方法...
}