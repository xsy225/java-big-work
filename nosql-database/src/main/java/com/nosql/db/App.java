package com.nosql.db;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nosql.db.index.IndexManager;
import com.nosql.db.server.NoSQLServer;
import com.nosql.db.storage.DatabaseEngine;
import com.nosql.db.storage.WriteAheadLog;
import com.nosql.db.utils.FileUtils;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        logger.info("启动NoSQL数据库应用");
        try {
            String dataDir = "data";
            logger.info("初始化数据目录: {}", dataDir);
            FileUtils.createDirectoryIfNotExists(dataDir);

            logger.info("初始化预写日志");
            WriteAheadLog wal = new WriteAheadLog(dataDir, "wal");

            logger.info("初始化索引管理器");
            IndexManager indexManager = new IndexManager(dataDir);

            logger.info("初始化数据库引擎");
            DatabaseEngine dbEngine = new DatabaseEngine(dataDir, indexManager, wal);

            logger.info("创建默认集合: users");
            dbEngine.createCollection("users");

            // 启动服务器
            logger.info("准备启动服务器，端口: 8888");
            NoSQLServer server = new NoSQLServer(8888, 10, dbEngine);
            server.start();
            logger.info("NoSQL服务器已成功启动，监听端口: 8888");

            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("接收到关闭信号，正在关闭服务器...");
                server.shutdown();
                logger.info("服务器已正常关闭");
            }));
        } catch (IOException e) {
            logger.error("服务器启动失败: {}", e.getMessage(), e);
            System.err.println("服务器启动失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
