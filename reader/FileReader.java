package reader;

/**
 * Created by carlosgarcia on 15/08/17.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;

public interface FileReader extends Closeable {

    Path getFilePath();
    FileSystem getFs();
}
