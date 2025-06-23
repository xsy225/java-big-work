package com.nosql.db.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;

public class NoSQLClient {
    private static final Logger logger = LoggerFactory.getLogger(NoSQLClient.class);
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
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            logger.info("已成功连接到服务器: {}:{}", host, port);
        } catch (IOException e) {
            logger.error("连接失败: 无法建立到 {}:{} 的连接。详细错误: {}", host, port, e.getMessage());
            throw e;
        }
    }

    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                sendCommand("EXIT", null, null, null);
                socket.close();
                logger.info("已断开与服务器的连接");
            }
        } catch (IOException e) {
            logger.error("断开连接时发生错误: {}", e.getMessage());
        }
    }

    private OperationResult sendCommand(String cmd, String collection, String id, Document doc) {
        Command request = new Command();
        request.command = cmd;
        request.collection = collection;
        request.id = id;
        request.document = doc;

        logger.debug("发送命令: {}", cmd);
        out.println(gson.toJson(request));

        try {
            String response = in.readLine();
            if (response == null) {
                logger.warn("服务器响应为空，可能已断开连接");
                return new OperationResult(false, "服务器响应为空，可能已断开连接");
            }

            try {
                OperationResult result = gson.fromJson(response, OperationResult.class);
                logger.debug("解析响应成功: {}", result.isSuccess() ? "成功" : "失败");
                return result;
            } catch (JsonParseException e) {
                logger.error("解析服务器响应失败: {}", response, e);
                return new OperationResult(false, "解析服务器响应失败: " + e.getMessage());
            }
        } catch (IOException e) {
            logger.error("通信错误: {}", e.getMessage());
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

    private OperationResult handleJsonCommand(String cmd, String collection, String jsonStr) {
        try {
            JsonParser.parseString(jsonStr);
            Document doc = gson.fromJson(jsonStr, Document.class);
            return (cmd.equals("INSERT")) ? insert(collection, doc) : update(collection, doc);
        } catch (JsonParseException e) {
            logger.error("无效的JSON格式: {}", e.getMessage());
            return new OperationResult(false, "无效的JSON格式: " + e.getMessage());
        } catch (Exception e) {
            logger.error("处理{}请求时发生异常: {}", cmd.toLowerCase(), e.getMessage());
            return new OperationResult(false, "处理请求时发生异常: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        String host = "localhost";
        int port = 8888;

        if (args.length >= 1) {
            host = args[0];
        }

        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                logger.error("无效的端口号: {}", args[1]);
                logger.info("使用默认端口: 8888");
            }
        }

        NoSQLClient client = new NoSQLClient(host, port);
        Scanner scanner = new Scanner(System.in);

        try {
            client.connect();
            logger.info("欢迎使用NoSQL数据库客户端！");
            logger.info("支持的命令:");
            logger.info("  INSERT <集合名> <JSON文档>");
            logger.info("  UPDATE <集合名> <JSON文档>");
            logger.info("  GET <集合名> <文档ID>");
            logger.info("  DELETE <集合名> <文档ID>");
            logger.info("  EXIT");

            while (true) {
                System.out.print("nosql> ");
                String input = scanner.nextLine().trim();
                if (input.isEmpty())
                    continue;

                String[] parts = input.split("\\s+", 2);
                if (parts.length < 1)
                    continue;

                String cmd = parts[0].toUpperCase();

                switch (cmd) {
                    case "INSERT":
                    case "UPDATE":
                        if (parts.length < 2) {
                            logger.error("缺少参数: 请提供集合名和{}数据", cmd.toLowerCase());
                            break;
                        }

                        String[] cmdParts = parts[1].split("\\s+", 2);
                        if (cmdParts.length < 2) {
                            logger.error("格式错误: 未找到有效的JSON文档");
                            break;
                        }

                        String collection = cmdParts[0];
                        String jsonStr = cmdParts[1];

                        OperationResult result = client.handleJsonCommand(cmd, collection, jsonStr);
                        logger.info("{}操作结果: {}", cmd,
                                result.isSuccess() ? "成功" : "失败: " + result.getMessage());
                        break;

                    case "GET":
                    case "DELETE":
                        if (parts.length < 2) {
                            logger.error("缺少参数: 请提供集合名和文档ID");
                            break;
                        }

                        String[] getDelParts = parts[1].split("\\s+", 2);
                        if (getDelParts.length < 2) {
                            logger.error("格式错误: 请提供有效的集合名和文档ID");
                            break;
                        }

                        String getDelCollection = getDelParts[0];
                        String docId = getDelParts[1];

                        OperationResult opResult =
                                (cmd.equals("GET")) ? client.get(getDelCollection, docId)
                                        : client.delete(getDelCollection, docId);

                        logger.info("{}操作结果: {}", cmd,
                                opResult.isSuccess()
                                        ? (cmd.equals("GET") ? "文档内容: " + opResult.getData() : "成功")
                                        : "失败: " + opResult.getMessage());
                        break;

                    case "EXIT":
                        client.disconnect();
                        return;

                    default:
                        logger.error("未知命令: {}", cmd);
                        logger.info("可用命令: INSERT, UPDATE, GET, DELETE, EXIT");
                }
            }
        } catch (IOException e) {
            logger.error("客户端运行错误: {}", e.getMessage());
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
