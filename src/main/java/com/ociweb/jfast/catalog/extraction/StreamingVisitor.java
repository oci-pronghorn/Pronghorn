package com.ociweb.jfast.catalog.extraction;

import java.nio.MappedByteBuffer;
import java.util.Arrays;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferWriter;

public class StreamingVisitor implements ExtractionVisitor {

    public static final int CATALOG_TEMPLATE_ID = 0;
    
    TypeTrie messageTypes;    
    FASTRingBuffer ringBuffer;
    
    byte[] catBytes;
    TemplateCatalogConfig catalog;
    
    long beforeDotValue;
    long accumDecimalValue;
    boolean aftetDot;
    
    //chars are written to  ring buffer.
    
    int bytePosStart;
    int byteMask      = ringBuffer.byteMask;
    byte[] byteBuffer = ringBuffer.byteBuffer;
    
    public StreamingVisitor(TypeTrie messageTypes, FASTRingBuffer ringBuffer) {
        
        this.messageTypes = messageTypes;
        this.ringBuffer = ringBuffer;
        
        messageTypes.restToRecordStart();
        
        
        bytePosStart = ringBuffer.addBytePos.value;
        aftetDot = false;
        beforeDotValue=0;
        accumDecimalValue=0;
    }
    
    
    
    @Override
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
                
        //discover the field types using the same way the previous visitor did it
        messageTypes.appendContent(mappedBuffer, pos, limit, contentQuoted);
           
        //keep bytes here in case we need it, will only be known after we are done
        int p = pos;
        while (p<limit) {
            
            //TODO: if there is a frame swith this visitor is repositioned to the front of the record again
            //so nothing needs to be kept we can roll back to the last known ring buffer position.
                        
            byte b = mappedBuffer.get(p);
            byteBuffer[byteMask&bytePosStart++] = b; //TODO: need to check for the right stop point
                        
            if ('.'==b) {
                aftetDot = true;
                beforeDotValue = accumDecimalValue;
                accumDecimalValue = 0;
            }
            
            accumDecimalValue = (10*accumDecimalValue) + (b-'0');  
  
            
            p++;
        }
              
                          
        
    }

    @Override
    public void closeRecord(int startPos) {
        
        // ** fields are now at the end of the record so the template Id is known
        
        //write it
        //flush it
        
    }

    @Override
    public void closeField() {
        //selecting the message type one field at at time as we move forward
        int fieldType = messageTypes.moveNextField();
        
        //this field type is only the simple type or null
        //we still do not know if its optional 
        //if it is null how do we know what to write to the ring buffer?
        //Ringbuffer takes, int, long, decimal, or bytes,, it must have a null value as well?
        
        
        
        // ** write as we go close out the field
        bytePosStart = ringBuffer.addBytePos.value;
        aftetDot = false;
        beforeDotValue = 0;
        accumDecimalValue = 0;
        
    }

    @Override
    public void closeFrame() {
      //TODO: Should we flush here?
        
    }


    @Override
    public void openFrame() {
        //get new catalog if is has been changed by the other visitor
        byte[] catBytes = messageTypes.getCatBytes();
        if (!Arrays.equals(this.catBytes, catBytes)) {

            this.catBytes = catBytes;        
            catalog = new TemplateCatalogConfig(catBytes);            
            //TODO: check assumption that templateID 0 is the one for sending catalogs.
                        
            //TODO: if any partial write of field data is in progress just throw it away becuase 
            //next frame will begin again from the start of the message.
            
            // Write new catalog to stream.
            FASTRingBufferWriter.writeInt(ringBuffer, CATALOG_TEMPLATE_ID);        
            FASTRingBufferWriter.writeBytes(ringBuffer, catBytes);        
        
            //TODO: if we moved the in progress field bytes they must be put back
            
            
        }        
    }

}
