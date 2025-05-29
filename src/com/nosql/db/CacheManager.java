package com.nosql.db;

import java.util.*;

// 缓存管理器
class CacheManager {
    private static final int CACHE_SIZE = 1000;
    private final LRUCache<String, Document> cache;
    
    public CacheManager() {
        this.cache = new LRUCache<>(CACHE_SIZE);
    }
    
    public Document get(Collection collection, String id) {
        String key = getCacheKey(collection, id);
        return cache.get(key);
    }
    
    public void put(Collection collection, Document document) {
        String key = getCacheKey(collection, document.getId());
        cache.put(key, document);
    }
    
    public void remove(Collection collection, String id) {
        String key = getCacheKey(collection, id);
        cache.remove(key);
    }
    
    private String getCacheKey(Collection collection, String id) {
        return collection.getDatabase().getName() + 
                "/" + collection.getName() + 
                "/" + id;
    }
    
    // LRU缓存实现...
    private static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int capacity;
        
        public LRUCache(int capacity) {
            super(capacity, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    return size() > capacity;
                }
            };
            this.capacity = capacity;
        }
    }
}