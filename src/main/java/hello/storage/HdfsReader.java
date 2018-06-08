package hello.storage;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;

public class HdfsReader extends Configured {

    public static final String FS_PARAM_NAME = "fs.defaultFS";
    protected String hadoopPath = "/etc/hadoop/conf";
    protected Configuration conf;
    protected FileSystem fs;



    public HdfsReader() {
        super();
        init();
    }

    public HdfsReader(Configuration conf) {
        super(conf);
        // TODO Auto-generated constructor stub
    }

    public int run(String inputHdfsPath, String outputLocalPath) throws Exception {

        Path inputPath = new Path(inputHdfsPath);
        //Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        //FileSystem fs = FileSystem.get(conf);
        InputStream is = fs.open(inputPath);
        OutputStream os = new BufferedOutputStream(new FileOutputStream(outputLocalPath));
        IOUtils.copyBytes(is, os, conf);
        return 0;
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