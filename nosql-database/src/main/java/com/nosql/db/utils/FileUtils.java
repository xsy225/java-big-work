package com.nosql.db.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static void createDirectoryIfNotExists(String path) throws IOException {
        logger.debug("检查目录是否存在: {}", path);
        Path dirPath = Paths.get(path);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            logger.info("创建目录: {}", path);
        } else {
            logger.debug("目录已存在: {}", path);
        }
    }

    public static List<String> readTextFileLines(String filePath) throws IOException {
        logger.debug("读取文件内容: {}", filePath);
        Path path = Paths.get(filePath);
        List<String> lines = Files.lines(path).collect(Collectors.toList());
        logger.debug("成功读取文件: {}, 行数: {}", filePath, lines.size());
        return lines;
    }
}
