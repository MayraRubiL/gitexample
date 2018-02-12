package reader;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by carlosgarcia on 14/08/17.
 */
public class TextFileReader extends AbstractFileReader{
    BufferedReader fileReader;

    public TextFileReader(FileSystem fs, Path filePath) throws IOException {
        super(fs, filePath);
        this.fileReader=new BufferedReader(new InputStreamReader(this.getFs().open(this.getFilePath())));
    }

    @Override
    public void printFile() throws IOException {
        String line;
        line=this.fileReader.readLine();
        while (line != null){
            System.out.println(line);
            line=this.fileReader.readLine();
        }
    }

    @Override
    public void close() throws IOException {
        this.fileReader.close();
    }
}
