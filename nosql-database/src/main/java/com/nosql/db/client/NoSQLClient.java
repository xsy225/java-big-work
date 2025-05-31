package com.nosql.db.client;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

import com.google.gson.Gson;
import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;

public class NoSQLClient {
    private final String host;
    private final int port;
    private final Gson gson = new Gson();
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public NoSQLClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws IOException {
        socket = new Socket(host, port);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        System.out.println("已连接到服务器: " + host + ":" + port);
    }

    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                sendCommand("EXIT", null, null, null);
                socket.close();
                System.out.println("已断开连接");
            }
        } catch (IOException e) {
            System.err.println("断开连接失败: " + e.getMessage());
        }
    }

    private OperationResult sendCommand(String cmd, String collection, String id, Document doc) {
        Command request = new Command();
        request.command = cmd;
        request.collection = collection;
        request.id = id;
        request.document = doc;
        
        out.println(gson.toJson(request));
        
        try {
            String response = in.readLine();
            return gson.fromJson(response, OperationResult.class);
        } catch (IOException e) {
            System.err.println("通信错误: " + e.getMessage());
            return new OperationResult(false, "通信失败");
        }
    }

    public OperationResult insert(String collection, Document doc) {
        return sendCommand("INSERT", collection, doc.getId(), doc);
    }

    public OperationResult get(String collection, String id) {
        return sendCommand("GET", collection, id, null);
    }

    public OperationResult update(String collection, Document doc) {
        return sendCommand("UPDATE", collection, doc.getId(), doc);
    }

    public OperationResult delete(String collection, String id) {
        return sendCommand("DELETE", collection, id, null);
    }

    public static void main(String[] args) {
        NoSQLClient client = new NoSQLClient("localhost", 8888);
        Scanner scanner = new Scanner(System.in);

        try {
            client.connect();
            System.out.println("欢迎使用NoSQL客户端！");
            System.out.println("支持的命令: INSERT, GET, UPDATE, DELETE, EXIT");

            while (true) {
                System.out.print("nosql> ");
                String input = scanner.nextLine().trim();
                if (input.isEmpty()) continue;

                String[] parts = input.split("\\s+", 2);
                String cmd = parts[0].toUpperCase();

                switch (cmd) {
                    case "INSERT":
                        if (parts.length < 2) {
                            System.out.println("用法: INSERT <集合名> <JSON文档>");
                            break;
                        }
                        String[] insertArgs = parts[1].split("\\s+", 2);
                        String coll = insertArgs[0];
                        String json = insertArgs[1];
                        Document doc = new Gson().fromJson(json, Document.class);
                        System.out.println(client.insert(coll, doc).getMessage());
                        break;

                    case "GET":
                        if (parts.length < 3) {
                            System.out.println("用法: GET <集合名> <文档ID>");
                            break;
                        }
                        String getColl = parts[1];
                        String getID = parts[2];
                        OperationResult getResult = client.get(getColl, getID);
                        System.out.println(getResult.isSuccess() ? "文档: " + getResult.getData() : "错误: " + getResult.getMessage());
                        break;

                    case "UPDATE":
                        if (parts.length < 2) {
                            System.out.println("用法: UPDATE <集合名> <JSON文档>");
                            break;
                        }
                        String[] updateArgs = parts[1].split("\\s+", 2);
                        String updateColl = updateArgs[0];
                        String updateJson = updateArgs[1];
                        Document updateDoc = new Gson().fromJson(updateJson, Document.class);
                        System.out.println(client.update(updateColl, updateDoc).getMessage());
                        break;

                    case "DELETE":
                        if (parts.length < 3) {
                            System.out.println("用法: DELETE <集合名> <文档ID>");
                            break;
                        }
                        String delColl = parts[1];
                        String delID = parts[2];
                        System.out.println(client.delete(delColl, delID).getMessage());
                        break;

                    case "EXIT":
                        client.disconnect();
                        return;

                    default:
                        System.out.println("未知命令，输入HELP查看帮助。");
                }
            }
        } catch (IOException e) {
            System.err.println("连接失败: " + e.getMessage());
        } finally {
            scanner.close();
        }
    }

    static class Command {
        String command;
        String collection;
        String id;
        Document document;
    }
}