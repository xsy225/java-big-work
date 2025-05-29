package com.nosql.db;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// 存储引擎 - 处理数据的持久化
class StorageEngine {
    private static final String DATA_DIR = "data";
    private static final long MAX_FILE_SIZE = 1024 * 1024; // 1MB
    private final Map<String, FileWriter> fileWriters = new ConcurrentHashMap<>();
    private final ExecutorService compressionThreadPool = Executors.newFixedThreadPool(2);
    
    public StorageEngine() {
        // 确保数据目录存在
        Path dataPath = Path.of(DATA_DIR);
        if (!Files.exists(dataPath)) {
            try {
                Files.createDirectories(dataPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    public void saveDocument(Collection collection, Document document) {
        try {
            String dbName = collection.getDatabase().getName();
            String collName = collection.getName();
            Path dirPath = Path.of(DATA_DIR, dbName, collName);
            Files.createDirectories(dirPath);
            
            String filePath = getCurrentFilePath(dirPath.toString());
            FileWriter writer = fileWriters.computeIfAbsent(filePath, 
                    path -> new FileWriter(path, true));
            
            // 写入文档
            writer.writeObject(document);
            
            // 检查文件大小并rotate
            checkAndRotateFile(writer, filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // 其他存储相关方法...
    
    private void checkAndRotateFile(FileWriter writer, String filePath) {
        File file = new File(filePath);
        if (file.length() >= MAX_FILE_SIZE) {
            try {
                writer.close();
                fileWriters.remove(filePath);
                
                // 异步压缩文件
                compressionThreadPool.submit(() -> compressFile(filePath));
                
                // 创建新文件
                String newFilePath = getNextFilePath(filePath);
                fileWriters.put(newFilePath, new FileWriter(newFilePath, true));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    private void compressFile(String filePath) {
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filePath));
             GZIPOutputStream gzos = new GZIPOutputStream(
                 new FileOutputStream(filePath + ".gz"))) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = bis.read(buffer)) != -1) {
                gzos.write(buffer, 0, bytesRead);
            }
            
            // 删除原始文件
            Files.delete(Path.of(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // 其他辅助方法...
}