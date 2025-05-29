package com.nosql.db;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

// 文档类 - 类似于关系型数据库中的行
class Document implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String id;
    private final Map<String, Object> fields = new LinkedHashMap<>();
    
    public Document(String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }
    
    public void put(String field, Object value) {
        fields.put(field, value);
    }
    
    public Object get(String field) {
        return fields.get(field);
    }
    
    public Map<String, Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }
    
    // 其他方法...
}