package com.nosql.db.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtils {
    public static void createDirectoryIfNotExists(String path) throws IOException {
        Path dirPath = Paths.get(path);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
    }

    public static List<String> readTextFileLines(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return Files.lines(path).collect(Collectors.toList());
    }
}