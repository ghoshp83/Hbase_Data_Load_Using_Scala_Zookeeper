package com.pralay.HbaseFullLoad;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class Environment {

    private static final Logger LOGGER = Logger.getLogger(Environment.class);
    private static final String usr=System.getProperty("user.name");

    // Removes the temporary folders aftrer Spark process
    public static void collectGarbage(String appId) {

        Configuration hadoopConf = new Configuration();
        String localDirs = hadoopConf.get("yarn.nodemanager.local-dirs");

        String pathPrefix = localDirs + "/usercache/"+usr+"/appcache/";
        String folderPrefix = "blockmgr-";

        File appDirectory = new File(pathPrefix + appId);

        for (File f : appDirectory.listFiles()) {
            // Pick a certain blockmgr-* folder from app's folder
            if (f.getName().startsWith(folderPrefix)) {

                // Walk through the folder and delete files
                Path subDirectory = Paths.get(pathPrefix + appId + "/" + f.getName());
                try {
                    Files.walkFileTree(subDirectory, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } catch (IOException e) {
                    LOGGER.error("Possible filesystem error during temporary file deletion!");
                    e.printStackTrace();
                }
            }
        }

        return;
    }
}
