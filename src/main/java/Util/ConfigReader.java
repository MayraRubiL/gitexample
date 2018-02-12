package Util;

import org.codehaus.jackson.annotate.JsonProperty;
import java.util.List;

/**
 * Created by carlosgarcia on 15/08/17.
 */
public class ConfigReader {

    @JsonProperty
    private String hdfsPath;
    @JsonProperty
    private List<Operation> operations;

    public String getHdfsPath(){
        return hdfsPath;
    }

    public List<Operation> getOperations(){
        return operations;
    }

}
