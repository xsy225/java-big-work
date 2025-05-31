package com.nosql.db.storage;

import com.nosql.db.index.IndexManager;
import com.nosql.db.utils.FileUtils;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Collection {
    private final String name;
    private final String dataDirectory;
    private final Map<String, Document> documents;
    private final ReadWriteLock lock;
    private final WriteAheadLog wal;
    private final IndexManager indexManager;

    public Collection(String name, String dataDirectory, WriteAheadLog wal, IndexManager indexManager) {
        this.name = name;
        this.dataDirectory = dataDirectory;
        this.documents = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.wal = wal;
        this.indexManager = indexManager;
        initCollectionDirectory();
        loadDocuments(); // 实际需实现磁盘加载
    }

    private void initCollectionDirectory() {
        try { FileUtils.createDirectoryIfNotExists(dataDirectory); } 
        catch (IOException e) { throw new RuntimeException("初始化集合目录失败: " + e.getMessage()); }
    }

    private void loadDocuments() {
        // 示例：模拟加载（实际需从文件系统读取）
        System.out.println("Loading documents for collection: " + name);
    }

    private void saveDocuments() {
        // 示例：模拟保存（实际需写入文件系统）
        System.out.println("Saving documents for collection: " + name);
    }

    // 增删改查方法
    public OperationResult insert(Document document) {
        lock.writeLock().lock();
        try {
            wal.write("INSERT", name, document.toJson());
            if (documents.containsKey(document.getId())) 
                return new OperationResult(false, "Document ID已存在: " + document.getId());
            documents.put(document.getId(), document);
            indexManager.updateIndex(name, document);
            return new OperationResult(true, "插入成功", document.getId());
        } finally { lock.writeLock().unlock(); }
    }

    public OperationResult update(Document document) {
        lock.writeLock().lock();
        try {
            wal.write("UPDATE", name, document.toJson());
            if (!documents.containsKey(document.getId())) 
                return new OperationResult(false, "Document不存在: " + document.getId());
            documents.put(document.getId(), document);
            indexManager.updateIndex(name, document);
            return new OperationResult(true, "更新成功", document.getId());
        } finally { lock.writeLock().unlock(); }
    }

    public OperationResult delete(String id) {
        lock.writeLock().lock();
        try {
            wal.write("DELETE", name, id);
            if (documents.remove(id) == null) 
                return new OperationResult(false, "Document不存在: " + id);
            indexManager.deleteFromIndex(name, id);
            return new OperationResult(true, "删除成功", id);
        } finally { lock.writeLock().unlock(); }
    }

    public OperationResult get(String id) {
        lock.readLock().lock();
        try {
            Document doc = documents.get(id);
            return doc != null ? new OperationResult(true, "查询成功", doc) : new OperationResult(false, "Document不存在: " + id);
        } finally { lock.readLock().unlock(); }
    }

    public OperationResult getAll() {
        lock.readLock().lock();
        try {
            List<Document> list = new ArrayList<>(documents.values());
            return new OperationResult(true, "查询到 " + list.size() + " 条记录", list);
        } finally { lock.readLock().unlock(); }
    }
}