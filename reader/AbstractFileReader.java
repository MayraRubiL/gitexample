package reader;

/**
 * Created by carlosgarcia on 15/08/17.
 */

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractFileReader<T> implements FileReader {

    private final FileSystem fs;
    private final Path filePath;

    public AbstractFileReader(FileSystem fs, Path filePath) {
        if (fs == null || filePath == null) {
            throw new IllegalArgumentException("fileSystem and filePath are required");
        }
        this.fs = fs;
        this.filePath = filePath;
    }

    @Override
    public FileSystem getFs() {
        return fs;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    public void printFile()throws IOException {}

}
