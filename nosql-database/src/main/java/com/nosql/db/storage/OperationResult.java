package com.nosql.db.storage;

import com.google.gson.Gson;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OperationResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Gson gson = new Gson(); // Gson实例

    private boolean success;
    private String message;
    private Object data;

    public OperationResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public OperationResult(boolean success, String message, Object data) {
        this(success, message);
        this.data = data;
    }

    // Getters
    public boolean isSuccess() { return success; }
    public String getMessage() { return message; }
    public Object getData() { return data; }

    // 转换为JSON字符串
    public String toJson() {
        return gson.toJson(this);
    }
}