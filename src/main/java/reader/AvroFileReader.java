package reader;

import org.apache.avro.file.SeekableInput;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.avro.file.FileReader;

import java.io.IOException;

/**
 * Created by carlosgarcia on 14/08/17.
 */
public class AvroFileReader extends AbstractFileReader<GenericRecord> {

    FileReader<GenericRecord> fileReader;

    public AvroFileReader(FileSystem fs, Path filePath) throws IOException {
        super(fs, filePath);

        SeekableInput input = new FsInput(this.getFilePath(), this.getFs());
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        this.fileReader = DataFileReader.openReader(input, reader);
    }

    @Override
    public void printFile() throws IOException {
        for (GenericRecord datum : this.fileReader) {
            System.out.println("value = " + datum);
        }
    }

    @Override
    public void close() throws IOException {
        this.fileReader.close();
    }
}
