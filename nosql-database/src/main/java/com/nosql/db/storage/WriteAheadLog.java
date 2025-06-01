package com.nosql.db.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLog {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final String LOG_FILE_EXTENSION = ".wal";
    private static final int MAX_LOG_SIZE = 1024 * 1024; // 1MB
    private final String logDirectory;
    private final String baseLogName;
    private File currentLogFile;
    private BufferedWriter writer;
    private final AtomicLong currentLogSize = new AtomicLong(0);
    private final Object writeLock = new Object();

    public WriteAheadLog(String logDirectory, String baseLogName) {
        this.logDirectory = logDirectory;
        this.baseLogName = baseLogName;
        initializeLogFile();
        logger.info("WAL初始化完成，目录: {}", logDirectory);
    }

    private void initializeLogFile() {
        try {
            Path dirPath = Paths.get(logDirectory);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
                logger.info("创建WAL目录: {}", logDirectory);
            }

            List<File> logFiles = getLogFiles();
            if (!logFiles.isEmpty()) {
                currentLogFile = logFiles.get(logFiles.size() - 1);
                currentLogSize.set(currentLogFile.length());
                logger.info("找到现有WAL文件: {}, 大小: {} 字节", currentLogFile.getName(),
                        currentLogSize.get());
            } else {
                currentLogFile = createNewLogFile();
                logger.info("创建新WAL文件: {}", currentLogFile.getName());
            }

            writer = new BufferedWriter(new FileWriter(currentLogFile, true));
        } catch (IOException e) {
            logger.error("初始化WAL失败: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize WAL", e);
        }
    }

    private File createNewLogFile() throws IOException {
        long timestamp = System.currentTimeMillis();
        File file = new File(logDirectory, baseLogName + "_" + timestamp + LOG_FILE_EXTENSION);
        if (!file.createNewFile()) {
            logger.error("创建WAL文件失败: {}", file.getAbsolutePath());
            throw new IOException("Failed to create log file: " + file.getAbsolutePath());
        }
        logger.info("创建新WAL文件: {}", file.getName());
        return file;
    }

    public void write(String operation, String collection, String data) {
        synchronized (writeLock) {
            try {
                if (currentLogSize.get()
                        + data.getBytes(StandardCharsets.UTF_8).length > MAX_LOG_SIZE) {
                    logger.info("WAL文件大小达到阈值，准备滚动: {}", currentLogFile.getName());
                    rotateLog();
                }
                String logEntry = String.format("%s|%s|%s%n", operation, collection, data);
                writer.write(logEntry);
                writer.flush();
                currentLogSize.addAndGet(logEntry.getBytes(StandardCharsets.UTF_8).length);
                logger.debug("WAL写入: {} {} ({}字节)", operation, collection, logEntry.length());
            } catch (IOException e) {
                logger.error("WAL写入失败: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to write to WAL", e);
            }
        }
    }

    private void rotateLog() throws IOException {
        writer.close();
        currentLogFile = createNewLogFile();
        writer = new BufferedWriter(new FileWriter(currentLogFile, true));
        currentLogSize.set(0);
        logger.info("WAL文件已滚动: {}", currentLogFile.getName());
    }

    public void recover(DatabaseEngine engine) throws IOException {
        List<File> logFiles = getLogFiles();
        logger.info("开始WAL恢复，文件数量: {}", logFiles.size());

        for (File logFile : logFiles) {
            logger.info("处理WAL文件: {}, 大小: {} 字节", logFile.getName(), logFile.length());
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line;
                int entryCount = 0;
                int successCount = 0;
                int failCount = 0;

                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|", 3);
                    if (parts.length < 3) {
                        logger.warn("无效WAL条目: {}", line);
                        failCount++;
                        continue;
                    }

                    String operation = parts[0];
                    String collection = parts[1];
                    String data = parts[2];
                    entryCount++;

                    try {
                        switch (operation) {
                            case "INSERT":
                                Document doc = Document.fromJson(data);
                                engine.insertDocument(collection, doc);
                                successCount++;
                                break;
                            case "UPDATE":
                                doc = Document.fromJson(data);
                                engine.updateDocument(collection, doc);
                                successCount++;
                                break;
                            case "DELETE":
                                engine.deleteDocument(collection, data);
                                successCount++;
                                break;
                            default:
                                logger.warn("未知WAL操作: {}", operation);
                                failCount++;
                        }
                    } catch (Exception e) {
                        logger.error("应用WAL条目失败: {}", line, e);
                        failCount++;
                    }
                }

                logger.info("WAL文件{}处理完成: 总条目={}, 成功={}, 失败={}", logFile.getName(), entryCount,
                        successCount, failCount);
            }
        }

        logger.info("WAL恢复完成，处理文件数量: {}", logFiles.size());
    }

    private List<File> getLogFiles() {
        File dir = new File(logDirectory);
        File[] files = dir.listFiles(
                (d, name) -> name.startsWith(baseLogName) && name.endsWith(LOG_FILE_EXTENSION));
        if (files == null) {
            logger.info("WAL目录中没有找到日志文件");
            return Collections.emptyList();
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        logger.info("找到{}个WAL文件", files.length);
        return Arrays.asList(files);
    }
}
