package com.ociweb.jfast.catalog.extraction;

import java.nio.MappedByteBuffer;

import com.ociweb.jfast.catalog.loader.ClientConfig;

public class FieldTypeVisitor implements ExtractionVisitor{

    private RecordFieldExtractor accumulatedMessageTypes;
           
    
    public FieldTypeVisitor(RecordFieldExtractor typeAccumulator) {        
        accumulatedMessageTypes = typeAccumulator;  
    }

    
    @Override
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
        accumulatedMessageTypes.appendContent(mappedBuffer, pos, limit, contentQuoted);
    }

    @Override
    public void closeRecord(int startPos) {
        accumulatedMessageTypes.appendNewRecord(startPos);
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
     
        accumulatedMessageTypes.mergeNumerics(0);   
        accumulatedMessageTypes.mergeOptionalNulls(0);
        accumulatedMessageTypes.mergeNumerics(0);   
        accumulatedMessageTypes.mergeOptionalNulls(0);//only works when there is 1 type and null so do other reductions first

        
        
        //TODO: add support to avoid printing null and avoid adding null to catalog
        // PRINT REPORT
        accumulatedMessageTypes.printRecursiveReport(0,"");
       System.err.println("total records: "+accumulatedMessageTypes.totalRecords+" tossed:"+accumulatedMessageTypes.tossedRecords);
        
        
        
        
    //    System.err.println(accumulatedMessageTypes.buildCatalog(true));
        
        accumulatedMessageTypes.memoizeCatBytes(); //store this so next visitor can pick it up on the open frame call.
        

        
        
    }
    
    
}
