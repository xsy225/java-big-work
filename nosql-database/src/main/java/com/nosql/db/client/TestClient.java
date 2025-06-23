package com.nosql.db.client;

import com.nosql.db.storage.Document;
import com.nosql.db.storage.OperationResult;

public class TestClient {
    public static void main(String[] args) {
        try {
            NoSQLClient client = new NoSQLClient("localhost", 8888);
            client.connect();

            // 测试插入文档
            Document doc = new Document();
            doc.put("name", "Test User");
            doc.put("age", 42);
            doc.put("email", "test@example.com");

            OperationResult insertResult = client.insert("test_collection", doc);
            System.out.println("插入结果: " + (insertResult.isSuccess() ? "成功" : "失败"));

            String docId = null;
            Document retrievedDoc = null;

            if (insertResult.isSuccess()) {
                docId = (String) insertResult.getData();
                System.out.println("文档ID: " + docId);

                // 测试查询文档
                OperationResult getResult = client.get("test_collection", docId);
                System.out.println("查询结果: " + (getResult.isSuccess() ? "成功" : "失败"));

                if (getResult.isSuccess()) {
                    retrievedDoc = (Document) getResult.getData();
                    System.out.println("查询到的文档: " + retrievedDoc);
                }
            }

            // 测试更新文档（增加空值检查）
            if (retrievedDoc != null && docId != null) {
                retrievedDoc.put("age", 43);
                OperationResult updateResult = client.update("test_collection", retrievedDoc);
                System.out.println("更新结果: " + (updateResult.isSuccess() ? "成功" : "失败"));

                // 测试删除文档
                OperationResult deleteResult = client.delete("test_collection", docId);
                System.out.println("删除结果: " + (deleteResult.isSuccess() ? "成功" : "失败"));
            }

            client.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
