package com.nosql.db.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class WriteAheadLog {
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
    }

    private void initializeLogFile() {
        try {
            Path dirPath = Paths.get(logDirectory);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            List<File> logFiles = getLogFiles();
            if (!logFiles.isEmpty()) {
                currentLogFile = logFiles.get(logFiles.size() - 1);
                currentLogSize.set(currentLogFile.length());
            } else {
                currentLogFile = createNewLogFile();
            }

            writer = new BufferedWriter(new FileWriter(currentLogFile, true));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize WAL", e);
        }
    }

    private File createNewLogFile() throws IOException {
        long timestamp = System.currentTimeMillis();
        File file = new File(logDirectory, baseLogName + "_" + timestamp + LOG_FILE_EXTENSION);
        if (!file.createNewFile()) {
            throw new IOException("Failed to create log file: " + file.getAbsolutePath());
        }
        return file;
    }

    public void write(String operation, String collection, String data) {
        synchronized (writeLock) {
            try {
                if (currentLogSize.get() + data.getBytes(StandardCharsets.UTF_8).length > MAX_LOG_SIZE) {
                    rotateLog();
                }
                String logEntry = String.format("%s|%s|%s%n", operation, collection, data);
                writer.write(logEntry);
                writer.flush();
                currentLogSize.addAndGet(logEntry.getBytes(StandardCharsets.UTF_8).length);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write to WAL", e);
            }
        }
    }

    private void rotateLog() throws IOException {
        writer.close();
        currentLogFile = createNewLogFile();
        writer = new BufferedWriter(new FileWriter(currentLogFile, true));
        currentLogSize.set(0);
    }

    public void recover(DatabaseEngine engine) throws IOException {
        List<File> logFiles = getLogFiles();
        for (File logFile : logFiles) {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|", 3);
                    if (parts.length < 3) continue;
                    
                    String operation = parts[0];
                    String collection = parts[1];
                    String data = parts[2];
                    
                    try {
                        switch (operation) {
                            case "INSERT":
                                Document doc = Document.fromJson(data);
                                engine.insertDocument(collection, doc);
                                break;
                            case "UPDATE":
                                doc = Document.fromJson(data);
                                engine.updateDocument(collection, doc);
                                break;
                            case "DELETE":
                                engine.deleteDocument(collection, data);
                                break;
                        }
                    } catch (Exception e) {
                        System.err.println("Error applying WAL entry: " + line);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private List<File> getLogFiles() {
        File dir = new File(logDirectory);
        File[] files = dir.listFiles((d, name) -> name.startsWith(baseLogName) && name.endsWith(LOG_FILE_EXTENSION));
        if (files == null) {
            return Collections.emptyList();
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        return Arrays.asList(files);
    }
}