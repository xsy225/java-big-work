package com.nosql.db;

import java.util.*;

// 索引管理器
class IndexManager {
    private final Map<String, Map<String, Set<String>>> indexes = new ConcurrentHashMap<>();
    
    public void updateIndex(Collection collection, Document document) {
        String dbName = collection.getDatabase().getName();
        String collName = collection.getName();
        
        // 为文档的每个字段创建索引
        document.getFields().forEach((field, value) -> {
            if (value != null) {
                String indexKey = getIndexKey(dbName, collName, field, value.toString());
                indexes.computeIfAbsent(indexKey, k -> ConcurrentHashMap.newKeySet())
                       .add(document.getId());
            }
        });
    }
    
    public void removeFromIndex(Collection collection, String documentId) {
        // 从所有相关索引中移除该文档ID
        indexes.values().forEach(ids -> ids.remove(documentId));
    }
    
    public Set<String> getDocumentsByIndex(String dbName, String collName, 
                                           String field, String value) {
        String indexKey = getIndexKey(dbName, collName, field, value);
        return indexes.getOrDefault(indexKey, Collections.emptySet());
    }
    
    private String getIndexKey(String dbName, String collName, 
                              String field, String value) {
        return dbName + "/" + collName + "/" + field + "=" + value;
    }
}