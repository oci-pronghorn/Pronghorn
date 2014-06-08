package com.ociweb.jfast.loader;

public class FieldReferenceOffsetManager {

    public final int preambleOffset; //-1 if there is no preamble
    public final int templateOffset;
    
    public FieldReferenceOffsetManager(TemplateCatalogConfig config) {
        
        //TODO: B, clientConfig must be able to skip reading the preamble,
        
        int pb = config.clientConfig.getPreableBytes();
        if (pb<=0) {
            preambleOffset = -1;
            templateOffset = 0;
        } else {
            preambleOffset = 0;
            templateOffset = (pb+3)>>2;
        }
               
        //TODO A. Do all this in a single pass.
        
        //must build array of fragment sizes based on script
        //must build jump points and rules from script
        //must build the offset for each field as this is done for each point in script
        //Put offests in map under "Name" and "Id"
        
        
    }

}
