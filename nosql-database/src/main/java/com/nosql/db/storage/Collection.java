package com.nosql.db.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nosql.db.index.IndexManager;
import com.nosql.db.utils.FileUtils;

public class Collection {
    private static final Logger logger = LoggerFactory.getLogger(Collection.class);
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
        loadDocuments();
        logger.info("集合{}初始化完成，文档数量: {}", name, documents.size());
    }

    // 插入方法：先检查ID是否存在，再写入WAL
    public OperationResult insert(Document document) {
        lock.writeLock().lock();
        try {
            // 关键修改：检查ID是否已存在
            if (documents.containsKey(document.getId())) {
                logger.warn("插入失败: 文档ID已存在 {}", document.getId());
                return new OperationResult(false, "Document ID已存在: " + document.getId());
            }

            // 写入WAL日志
            wal.write("INSERT", name, document.toJson());

            // 插入文档并更新索引
            documents.put(document.getId(), document);
            indexManager.updateIndex(name, document);

            logger.info("成功插入文档到集合{}，ID: {}", name, document.getId());
            return new OperationResult(true, "插入成功", document.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 其他方法保持不变
    public OperationResult update(Document document) {
        lock.writeLock().lock();
        try {
            wal.write("UPDATE", name, document.toJson());
            if (!documents.containsKey(document.getId())) {
                return new OperationResult(false, "Document不存在: " + document.getId());
            }
            documents.put(document.getId(), document);
            indexManager.updateIndex(name, document);
            return new OperationResult(true, "更新成功", document.getId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public OperationResult delete(String id) {
        lock.writeLock().lock();
        try {
            wal.write("DELETE", name, id);
            if (documents.remove(id) == null) {
                return new OperationResult(false, "Document不存在: " + id);
            }
            indexManager.deleteFromIndex(name, id);
            return new OperationResult(true, "删除成功", id);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public OperationResult get(String id) {
        lock.readLock().lock();
        try {
            Document doc = documents.get(id);
            if (doc != null) {
                return new OperationResult(true, "查询成功", doc);
            } else {
                return new OperationResult(false, "Document不存在: " + id);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public OperationResult getAll() {
        lock.readLock().lock();
        try {
            List<Document> list = new ArrayList<>(documents.values());
            return new OperationResult(true, "查询到 " + list.size() + " 条记录", list);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void initCollectionDirectory() {
        try {
            FileUtils.createDirectoryIfNotExists(dataDirectory);
        } catch (IOException e) {
            throw new RuntimeException("初始化集合目录失败: " + e.getMessage());
        }
    }

    private void loadDocuments() {
        // 实际实现中应从文件系统加载文档
        logger.info("加载集合{}的文档", name);
    }

    private void saveDocuments() {
        // 实际实现中应保存文档到文件系统
        logger.debug("保存集合{}的文档", name);
    }
}