package com.nosql.db;

import java.util.*;

// 数据库类
class Database {
    private final String name;
    private final Map<String, Collection> collections = new ConcurrentHashMap<>();
    
    public Database(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public Collection getCollection(String collectionName) {
        return collections.computeIfAbsent(collectionName, 
                name -> new Collection(name, this));
    }
    
    public void deleteCollection(String collectionName) {
        collections.remove(collectionName);
    }
    
    public Set<String> getCollectionNames() {
        return collections.keySet();
    }
}