package com.nosql.db.storage;

import com.nosql.db.index.IndexManager;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseEngine {
    private final String dataDirectory;
    private final IndexManager indexManager;
    private final WriteAheadLog wal;
    private final Map<String, Collection> collections = new ConcurrentHashMap<>();

    public DatabaseEngine(String dataDirectory, IndexManager indexManager, WriteAheadLog wal) {
        this.dataDirectory = dataDirectory;
        this.indexManager = indexManager;
        this.wal = wal;
    }

    // 创建集合
    public OperationResult createCollection(String collectionName) {
        if (collections.containsKey(collectionName)) 
            return new OperationResult(false, "集合已存在: " + collectionName);
        Collection coll = new Collection(collectionName, 
                                         dataDirectory + "/" + collectionName, 
                                         wal, 
                                         indexManager);
        collections.put(collectionName, coll);
        return new OperationResult(true, "集合创建成功: " + collectionName);
    }

    // 获取集合
    public Collection getCollection(String collectionName) {
        return collections.get(collectionName);
    }

    // 增删改查方法（委托给Collection）
    public OperationResult insertDocument(String collectionName, Document document) {
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.insert(document) : new OperationResult(false, "集合不存在: " + collectionName);
    }

    public OperationResult updateDocument(String collectionName, Document document) {
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.update(document) : new OperationResult(false, "集合不存在: " + collectionName);
    }

    public OperationResult deleteDocument(String collectionName, String documentId) {
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.delete(documentId) : new OperationResult(false, "集合不存在: " + collectionName);
    }

    // WAL 恢复
    public void recoverFromWal() {
        try { wal.recover(this); } 
        catch (IOException e) { System.err.println("WAL恢复失败: " + e.getMessage()); }
    }

    // 新增方法（修复ClientHandler的调用）
    public OperationResult getDocument(String collectionName, String id) {
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.get(id) : new OperationResult(false, "集合不存在: " + collectionName);
    }

    public OperationResult getAllDocuments(String collectionName) {
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.getAll() : new OperationResult(false, "集合不存在: " + collectionName);
    }
}