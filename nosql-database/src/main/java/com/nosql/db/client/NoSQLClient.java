package com.nosql.db.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

import com.google.gson.Gson;
import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;

/**
 * NoSQL数据库的客户端类，负责连接到服务器并发送操作请求。
 * 此类不包含任何服务器启动逻辑，仅作为客户端与已运行的服务器通信。
 */
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

    /**
     * 连接到指定的服务器
     * 
     * @throws IOException 如果连接失败（例如服务器未运行或端口被占用）
     */
    public void connect() throws IOException {
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("已成功连接到服务器: " + host + ":" + port);
        } catch (IOException e) {
            System.err.println("连接失败: 无法建立到 " + host + ":" + port + " 的连接。");
            System.err.println("请确保服务器已启动且端口未被占用。详细错误: " + e.getMessage());
            throw e;
        }
    }

    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                sendCommand("EXIT", null, null, null);
                socket.close();
                System.out.println("已断开与服务器的连接");
            }
        } catch (IOException e) {
            System.err.println("断开连接时发生错误: " + e.getMessage());
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
            if (response == null) {
                return new OperationResult(false, "服务器响应为空，可能已断开连接");
            }
            return gson.fromJson(response, OperationResult.class);
        } catch (IOException e) {
            System.err.println("通信错误: " + e.getMessage());
            return new OperationResult(false, "与服务器通信失败");
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
        // 检查命令行参数是否包含主机和端口
        String host = "localhost";
        int port = 8888;

        if (args.length >= 1) {
            host = args[0];
        }

        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("无效的端口号: " + args[1]);
                System.err.println("使用默认端口: 8888");
            }
        }

        NoSQLClient client = new NoSQLClient(host, port);
        Scanner scanner = new Scanner(System.in);

        try {
            client.connect();
            System.out.println("欢迎使用NoSQL数据库客户端！");
            System.out.println("支持的命令: INSERT, GET, UPDATE, DELETE, EXIT");

            while (true) {
                System.out.print("nosql> ");
                String input = scanner.nextLine().trim();
                if (input.isEmpty())
                    continue;

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
                        OperationResult insertResult = client.insert(coll, doc);
                        System.out.println(insertResult.isSuccess() ? "插入成功" : "插入失败: " + insertResult.getMessage());
                        break;

                    case "GET":
                        if (parts.length < 3) {
                            System.out.println("用法: GET <集合名> <文档ID>");
                            break;
                        }
                        String getColl = parts[1];
                        String getID = parts[2];
                        OperationResult getResult = client.get(getColl, getID);
                        System.out.println(getResult.isSuccess() ? "文档内容: " + getResult.getData()
                                : "获取失败: " + getResult.getMessage());
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
                        OperationResult updateResult = client.update(updateColl, updateDoc);
                        System.out.println(updateResult.isSuccess() ? "更新成功" : "更新失败: " + updateResult.getMessage());
                        break;

                    case "DELETE":
                        if (parts.length < 3) {
                            System.out.println("用法: DELETE <集合名> <文档ID>");
                            break;
                        }
                        String delColl = parts[1];
                        String delID = parts[2];
                        OperationResult deleteResult = client.delete(delColl, delID);
                        System.out.println(deleteResult.isSuccess() ? "删除成功" : "删除失败: " + deleteResult.getMessage());
                        break;

                    case "EXIT":
                        client.disconnect();
                        return;

                    default:
                        System.out.println("未知命令。可用命令: INSERT, GET, UPDATE, DELETE, EXIT");
                }
            }
        } catch (IOException e) {
            System.err.println("客户端运行错误: " + e.getMessage());
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