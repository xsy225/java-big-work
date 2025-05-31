package com.nosql.db;

import com.nosql.db.index.IndexManager;
import com.nosql.db.server.NoSQLServer; // 确保导入正确
import com.nosql.db.storage.DatabaseEngine;
import com.nosql.db.storage.WriteAheadLog;
import com.nosql.db.utils.FileUtils;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        try {
            String dataDir = "data";
            FileUtils.createDirectoryIfNotExists(dataDir);
            
            WriteAheadLog wal = new WriteAheadLog(dataDir, "wal");
            IndexManager indexManager = new IndexManager(dataDir);
            DatabaseEngine dbEngine = new DatabaseEngine(dataDir, indexManager, wal);
            
            dbEngine.createCollection("users");
            
            // 确保类名和路径正确
            NoSQLServer server = new NoSQLServer(8888, 10, dbEngine);
            server.start();
            System.out.println("NoSQL服务器已启动，端口: 8888");
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("正在关闭服务器...");
                server.shutdown();
            }));
        } catch (IOException e) {
            System.err.println("服务器启动失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}