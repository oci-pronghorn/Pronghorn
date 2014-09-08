package com.ociweb.jfast.catalog.extraction;

import java.nio.MappedByteBuffer;

import com.ociweb.jfast.catalog.loader.ClientConfig;

public class FieldTypeVisitor implements ExtractionVisitor{

    private TypeTrie accumulatedMessageTypes;
           
    
    public FieldTypeVisitor() {        
        accumulatedMessageTypes = new TypeTrie();     
    }

    
    @Override
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
        accumulatedMessageTypes.appendContent(mappedBuffer, pos, limit, contentQuoted);
    }

    @Override
    public void closeRecord() {
        accumulatedMessageTypes.appendNewRecord();
    }

    @Override
    public void closeField() {
        accumulatedMessageTypes.appendNewField();     
    }

    @Override
    public void openFrame() {
        //has nothing to do
    }

    @Override
    public void closeFrame() {
        
        
        System.err.println("________________________________________");
        
        
        //TODO: add boolean config for this step. Normally there will be no consolidation of types because we want
        //      as many message types as possible to help with the next stage.  Sometimes when we only have one null
        //      along with one field it makes more sence to colapse thse together.
        accumulatedMessageTypes.mergeOptionalNulls(0);
        
        // PRINT REPORT
        accumulatedMessageTypes.printRecursiveReport(0,"");
        
        accumulatedMessageTypes.memoizeCatBytes(); //store this so next visitor can pick it up on the open frame call.
        


        
    }




    
    
}
