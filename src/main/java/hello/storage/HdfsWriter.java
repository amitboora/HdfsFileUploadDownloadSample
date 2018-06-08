package hello.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class HdfsWriter extends Configured {

    public static final String FS_PARAM_NAME = "fs.defaultFS";
    protected String hadoopPath = "/etc/hadoop/conf";
    protected Configuration conf;
    protected FileSystem fs;



    public HdfsWriter() {
        super();
        init();
    }

    public HdfsWriter(Configuration conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    public void store(MultipartFile file, String outputHdfsPath) {

        try {
            String filename = StringUtils.cleanPath(file.getOriginalFilename());
            Path outputPath = new Path(outputHdfsPath);

            if (file.isEmpty()) {
                throw new StorageException("Failed to store empty file " + filename);
            }
            if (filename.contains("..")) {
                // This is a security check
                throw new StorageException(
                        "Cannot store file with relative path outside current directory "
                                + filename);
            }
            try (InputStream inputStream = file.getInputStream()) {
                OutputStream os = fs.create(outputPath);
                InputStream is = new BufferedInputStream(inputStream);
                IOUtils.copyBytes(is, os, conf);
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }



    protected void init(){
        conf = new Configuration();
        conf.addResource(new Path(hadoopPath + "/core-site.xml"));
        conf.addResource(new Path(hadoopPath + "/hdfs-site.xml"));
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        try {
            fs = FileSystem.get(conf);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

}
