package Util;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by carlosgarcia on 28/08/17.
 */
public class Operation {

    @JsonProperty
    private String type;
    @JsonProperty
    private String inputPath;

    public String getType(){
        return type;
    }

    public String getInputPath(){
        return inputPath;
    }
}
