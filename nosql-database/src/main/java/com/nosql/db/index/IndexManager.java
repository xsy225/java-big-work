package com.nosql.db.index;

import com.nosql.db.storage.Document;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class IndexManager {
    private final Map<String, Map<String, Map<String, Set<String>>>> indexes; // collection -> field -> value -> docIds

    public IndexManager(String dataDirectory) {
        this.indexes = new ConcurrentHashMap<>();
    }

    public void createIndex(String collectionName, String fieldName) {
        indexes.computeIfAbsent(collectionName, k -> new ConcurrentHashMap<>())
               .put(fieldName, new ConcurrentHashMap<>());
    }

    public void updateIndex(String collectionName, Document document) {
        Map<String, Map<String, Set<String>>> collIndexes = indexes.get(collectionName);
        if (collIndexes == null) return;
        
        String docId = document.getId();
        for (Map.Entry<String, Map<String, Set<String>>> entry : collIndexes.entrySet()) {
            String field = entry.getKey();
            Map<String, Set<String>> valueMap = entry.getValue();
            
            // 先删除旧的索引
            for (Set<String> ids : valueMap.values()) {
                ids.remove(docId);
            }
            
            // 添加新的索引
            Object value = document.get(field);
            if (value != null) {
                String strValue = value.toString();
                valueMap.computeIfAbsent(strValue, k -> new ConcurrentSkipListSet<>())
                         .add(docId);
            }
        }
    }

    public void deleteFromIndex(String collectionName, String docId) {
        Map<String, Map<String, Set<String>>> collIndexes = indexes.getOrDefault(collectionName, Collections.emptyMap());
        collIndexes.values().forEach(fieldMap -> fieldMap.values().forEach(ids -> ids.remove(docId)));
    }

    public Set<String> getDocumentIds(String collectionName, String fieldName, Object value) {
        Map<String, Map<String, Set<String>>> collIndexes = indexes.getOrDefault(collectionName, Collections.emptyMap());
        return collIndexes.getOrDefault(fieldName, Collections.emptyMap())
                         .getOrDefault(value.toString(), Collections.emptySet());
    }
}