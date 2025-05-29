package com.nosql.db;

import java.util.*;

// 集合类 - 类似于关系型数据库中的表
class Collection {
    private final String name;
    private final Database database;
    private final Map<String, Document> documents = new ConcurrentHashMap<>();
    
    public Collection(String name, Database database) {
        this.name = name;
        this.database = database;
    }
    
    public String getName() {
        return name;
    }
    
    public Database getDatabase() {
        return database;
    }
    
    public void insert(Document document) {
        documents.put(document.getId(), document);
        // 记录日志
        database.getServer().getLogManager().logInsert(this, document);
        // 更新索引
        database.getServer().getIndexManager().updateIndex(this, document);
        // 持久化
        database.getServer().getStorageEngine().saveDocument(this, document);
    }
    
    public Document get(String id) {
        // 先从缓存获取
        Document document = database.getServer().getCacheManager().get(this, id);
        if (document != null) {
            return document;
        }
        
        // 从存储获取
        document = database.getServer().getStorageEngine().loadDocument(this, id);
        if (document != null) {
            // 放入缓存
            database.getServer().getCacheManager().put(this, document);
        }
        return document;
    }
    
    public void update(Document document) {
        documents.put(document.getId(), document);
        // 记录日志
        database.getServer().getLogManager().logUpdate(this, document);
        // 更新索引
        database.getServer().getIndexManager().updateIndex(this, document);
        // 持久化
        database.getServer().getStorageEngine().saveDocument(this, document);
    }
    
    public void delete(String id) {
        documents.remove(id);
        // 记录日志
        database.getServer().getLogManager().logDelete(this, id);
        // 更新索引
        database.getServer().getIndexManager().removeFromIndex(this, id);
        // 持久化
        database.getServer().getStorageEngine().deleteDocument(this, id);
    }
    
    // 批量操作方法...
}