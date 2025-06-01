package com.nosql.db.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.nosql.db.index.IndexManager;

public class DatabaseEngine {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseEngine.class);
    private final String dataDirectory;
    private final IndexManager indexManager;
    private final WriteAheadLog wal;
    private final Map<String, Collection> collections = new ConcurrentHashMap<>();

    public DatabaseEngine(String dataDirectory, IndexManager indexManager, WriteAheadLog wal) {
        this.dataDirectory = dataDirectory;
        this.indexManager = indexManager;
        this.wal = wal;
        logger.info("数据库引擎初始化完成，数据目录: {}", dataDirectory);
    }

    // 创建集合
    public OperationResult createCollection(String collectionName) {
        logger.info("尝试创建集合: {}", collectionName);
        if (collections.containsKey(collectionName)) {
            logger.warn("创建集合失败: 集合已存在 {}", collectionName);
            return new OperationResult(false, "集合已存在: " + collectionName);
        }

        Collection coll = new Collection(collectionName, dataDirectory + "/" + collectionName, wal,
                indexManager);
        collections.put(collectionName, coll);
        logger.info("集合创建成功: {}", collectionName);
        return new OperationResult(true, "集合创建成功: " + collectionName);
    }

    // 获取集合
    public Collection getCollection(String collectionName) {
        logger.debug("获取集合: {}", collectionName);
        return collections.get(collectionName);
    }

    // 增删改查方法（委托给Collection）
    public OperationResult insertDocument(String collectionName, Document document) {
        logger.info("尝试插入文档到集合: {}, ID: {}", collectionName, document.getId());
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.insert(document)
                : new OperationResult(false, "集合不存在: " + collectionName);
    }

    public OperationResult updateDocument(String collectionName, Document document) {
        logger.info("尝试更新集合: {} 中的文档, ID: {}", collectionName, document.getId());
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.update(document)
                : new OperationResult(false, "集合不存在: " + collectionName);
    }

    public OperationResult deleteDocument(String collectionName, String documentId) {
        logger.info("尝试删除集合: {} 中的文档, ID: {}", collectionName, documentId);
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.delete(documentId)
                : new OperationResult(false, "集合不存在: " + collectionName);
    }

    // WAL 恢复
    public void recoverFromWal() {
        logger.info("开始从WAL恢复数据");
        try {
            wal.recover(this);
            logger.info("WAL恢复完成");
        } catch (Exception e) {
            logger.error("WAL恢复失败: {}", e.getMessage(), e);
            System.err.println("WAL恢复失败: " + e.getMessage());
        }
    }

    // 新增方法（修复ClientHandler的调用）
    public OperationResult getDocument(String collectionName, String id) {
        logger.info("尝试获取集合: {} 中的文档, ID: {}", collectionName, id);
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.get(id) : new OperationResult(false, "集合不存在: " + collectionName);
    }

    public OperationResult getAllDocuments(String collectionName) {
        logger.info("尝试获取集合: {} 中的所有文档", collectionName);
        Collection coll = collections.get(collectionName);
        return coll != null ? coll.getAll()
                : new OperationResult(false, "集合不存在: " + collectionName);
    }
}
