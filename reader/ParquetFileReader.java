package reader;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;

/**
 * Created by carlosgarcia on 14/08/17.
 */
public class ParquetFileReader extends AbstractFileReader{

    ParquetReader fileReader;

    public ParquetFileReader(FileSystem fs, Path filePath) throws IOException {
        super(fs, filePath);

        this.fileReader = new AvroParquetReader(this.getFs().getConf(),this.getFilePath());
    }

    @Override
    public void printFile() throws IOException {
        GenericRecord record = null;
        while ((record = (GenericRecord) this.fileReader.read()) != null) {
            System.out.println("value = " + record);
        }
    }

    @Override
    public void close() throws IOException {
        this.fileReader.close();
    }
}
