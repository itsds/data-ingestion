package com.ds.code;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ExtractAndUploadToHDFS {
    public static void main(String[] args) throws IOException {
        unzip();
        uploadFiles();
    }

    public static void unzip() {
        String[] srcFiles = new String[]{"IND_adm.zip", "IND_alt.zip", "IND_cov.zip", "IND_gaz.zip", "IND_pop.zip", "IND_rds.zip", "IND_rrd.zip", "IND_wat.zip"};
        String localSource = "/Users/Durga.Shanker/Documents/hackathon-R2/ds/geo_data/india/";
        String localDestination = "/Users/Durga.Shanker/Documents/hackathon-R2/ds/geo_data/india/extracted/";

        int len = srcFiles.length;
        int i = 0;
        while (i < len) {
            String location = localSource + srcFiles[i];
            String dest = localDestination + srcFiles[i].replace(".zip", "");
            try {
                ZipFile zipFile = new ZipFile(location);
                zipFile.extractAll(dest);
            } catch (ZipException e) {
                e.printStackTrace();
            }
            i++;
        }
    }

    public static void uploadFiles() throws IOException {
        String[] srcFiles = new String[]{"IND_adm", "IND_alt", "IND_cov", "IND_gaz", "IND_pop", "IND_rds", "IND_rrd", "IND_wat"};
        String hdfsUri = "hdfs://localhost:9000";
        String localPath = "/Users/Durga.Shanker/Documents/hackathon-R2/ds/geo_data/india/extracted/";
        String hdfsPath = "/user/Durga.Shanker/geo_data/india_22062020/";
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), configuration);

        int len = srcFiles.length;
        int i = 0;
        while (i < len) {
            String updatedLocalPath = localPath + srcFiles[i];
            String updatedHdfsPath = hdfsPath + srcFiles[i];
            fs.copyFromLocalFile(new Path(updatedLocalPath), new Path(updatedHdfsPath));
            i++;
        }
    }
}