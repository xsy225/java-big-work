package com.nosql.db.storage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class Document implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(Document.class);
    private static final long serialVersionUID = 1L;
    private static final Gson gson = new GsonBuilder().create();

    private String id;
    private Map<String, Object> data;
    private long createdAt;
    private long updatedAt;

    public Document() {
        this.id = UUID.randomUUID().toString();
        this.data = new HashMap<>();
        this.createdAt = System.currentTimeMillis();
        this.updatedAt = this.createdAt;
        logger.debug("创建新文档，ID: {}", id);
    }

    public Document(Map<String, Object> data) {
        this(); // 调用无参构造生成ID
        // 移除用户传入的"id"字段，避免覆盖自动生成的ID
        data.remove("id");
        this.data = new HashMap<>(data);
        logger.debug("使用数据创建新文档，ID: {}", id);
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    // 设为私有方法，禁止外部修改ID
    private void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
        this.updatedAt = System.currentTimeMillis();
    }

    public Object get(String fieldName) {
        return data.get(fieldName);
    }

    public void put(String fieldName, Object value) {
        data.put(fieldName, value);
        this.updatedAt = System.currentTimeMillis();
    }

    // Serialization
    public String toJson() {
        return gson.toJson(this);
    }

    public static Document fromJson(String json) {
        try {
            return gson.fromJson(json, Document.class);
        } catch (JsonSyntaxException e) {
            throw new IllegalArgumentException("Invalid JSON: " + e.getMessage());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Document document = (Document) o;
        return id.equals(document.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return toJson();
    }
}