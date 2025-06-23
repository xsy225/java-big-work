package com.nosql.db.index;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nosql.db.storage.Document;

public class IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
    private final Map<String, Map<String, Map<String, Set<String>>>> indexes;

    public IndexManager(String dataDirectory) {
        this.indexes = new ConcurrentHashMap<>();
        logger.info("索引管理器初始化完成");
    }

    public void createIndex(String collectionName, String fieldName) {
        indexes.computeIfAbsent(collectionName, k -> new ConcurrentHashMap<>()).put(fieldName,
                new ConcurrentHashMap<>());
        logger.info("为集合{}的字段{}创建索引", collectionName, fieldName);
    }

    public void updateIndex(String collectionName, Document document) {
        Map<String, Map<String, Set<String>>> collIndexes = indexes.get(collectionName);
        if (collIndexes == null) {
            logger.debug("集合{}没有索引，跳过更新", collectionName);
            return;
        }

        String docId = document.getId();
        logger.debug("更新集合{}中文档{}的索引", collectionName, docId);

        for (Map.Entry<String, Map<String, Set<String>>> entry : collIndexes.entrySet()) {
            String field = entry.getKey();
            Map<String, Set<String>> valueMap = entry.getValue();

            for (Set<String> ids : valueMap.values()) {
                ids.remove(docId);
            }

            Object value = document.get(field);
            if (value != null) {
                String strValue = value.toString();
                valueMap.computeIfAbsent(strValue, k -> new ConcurrentSkipListSet<>()).add(docId);
                logger.trace("为集合{}的字段{}添加索引: 值={}, 文档ID={}", collectionName, field, strValue,
                        docId);
            }
        }
    }

    public void deleteFromIndex(String collectionName, String docId) {
        Map<String, Map<String, Set<String>>> collIndexes =
                indexes.getOrDefault(collectionName, Collections.emptyMap());
        if (collIndexes.isEmpty()) {
            logger.debug("集合{}没有索引，跳过删除", collectionName);
            return;
        }

        logger.debug("从集合{}的索引中删除文档{}", collectionName, docId);
        collIndexes.values()
                .forEach(fieldMap -> fieldMap.values().forEach(ids -> ids.remove(docId)));
    }

    public Set<String> getDocumentIds(String collectionName, String fieldName, Object value) {
        Map<String, Map<String, Set<String>>> collIndexes =
                indexes.getOrDefault(collectionName, Collections.emptyMap());
        Set<String> result = collIndexes.getOrDefault(fieldName, Collections.emptyMap())
                .getOrDefault(value.toString(), Collections.emptySet());
        logger.debug("从集合{}的字段{}获取索引值={}的文档ID，结果数量: {}", collectionName, fieldName, value,
                result.size());
        return result;
    }
}
