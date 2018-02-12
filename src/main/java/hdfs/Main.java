package hdfs;

import Util.ConfigReader;
import Util.Operation;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import reader.AvroFileReader;
import reader.ParquetFileReader;
import reader.TextFileReader;
import reader.AbstractFileReader;
import java.io.File;

import java.util.logging.Logger;

public class Main {

    private static final Logger logger = Logger.getLogger("hdfs.Main");

    public static void main(String[] args) throws Exception {

        if (args.length<1) {
            logger.severe("1 arg is required :\n\t- Config file path");
            System.err.println("1 arg is required :\n\t- Config file path");
            System.exit(128);
        }

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ConfigReader configReader = mapper.readValue(new File(args[0]), ConfigReader.class);

        String hdfsUri = configReader.getHdfsPath();


        Configuration config = new Configuration(); // make this your Hadoop env config
        config.set("fs.defaultFS", hdfsUri);
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");

        FileSystem fs = FileSystem.get(config);
        AbstractFileReader reader;

        for (Operation op : configReader.getOperations()) {
            System.out.println("Reading file type: "+op.getType());

            switch (op.getType()) {
                case "Text":
                    reader = new TextFileReader(fs, new Path(op.getInputPath()));
                    reader.printFile();
                    reader.close();
                    break;
                case "Avro":
                    reader = new AvroFileReader(fs, new Path(op.getInputPath()));
                    reader.printFile();
                    reader.close();
                    break;
                case "Parquet":
                    reader = new ParquetFileReader(fs, new Path(op.getInputPath()));
                    reader.printFile();
                    reader.close();
                    break;
                default:
                    System.out.println("Operation "+op.getType()+" not allowed");
            }

        }
    }
}
