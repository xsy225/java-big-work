package com.nosql.db.storage;

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

public class OperationResult implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(OperationResult.class);
    private static final long serialVersionUID = 1L;
    private static final Gson gson = new Gson();

    private boolean success;
    private String message;
    private Object data;

    public OperationResult(boolean success, String message) {
        this.success = success;
        this.message = message;
        logger.debug("创建操作结果: 成功={}, 消息={}", success, message);
    }

    public OperationResult(boolean success, String message, Object data) {
        this(success, message);
        this.data = data;
        logger.debug("创建操作结果: 成功={}, 消息={}, 数据类型={}", success, message,
                data != null ? data.getClass().getSimpleName() : "null");
    }

    // Getters
    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Object getData() {
        return data;
    }

    // 转换为JSON字符串
    public String toJson() {
        String json = gson.toJson(this);
        logger.trace("将操作结果转换为JSON: {}", json);
        return json;
    }
}
