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
    
    int bytePosActive;
    int bytePosStartField;
    int byteMask;
    byte[] byteBuffer;
    
    public StreamingVisitor(TypeTrie messageTypes, FASTRingBuffer ringBuffer) {
        
        this.messageTypes = messageTypes;
        this.ringBuffer = ringBuffer;        
        this.byteMask      = ringBuffer.byteMask;
        this.byteBuffer = ringBuffer.byteBuffer;
        
        messageTypes.restToRecordStart();
        
        bytePosStartField = bytePosActive = ringBuffer.addBytePos.value;
        aftetDot = false;
        beforeDotValue=0;
        accumDecimalValue=0;
    }
        
    
    @Override
    public void appendContent(MappedByteBuffer mappedBuffer, int pos, int limit, boolean contentQuoted) {
                
        //discovering the field types using the same way the previous visitor did it
        messageTypes.appendContent(mappedBuffer, pos, limit, contentQuoted);
                  
        //keep bytes here in case we need it, will only be known after we are done
        int p = pos;
        while (p<limit) {
            byte b = mappedBuffer.get(p);
            byteBuffer[byteMask&bytePosActive++] = b; //TODO: need to check for the right stop point
                        
            if ('.'==b) {
                aftetDot = true;
                beforeDotValue = accumDecimalValue;
                accumDecimalValue = 0;
            } else {
                //TODO: add conditional to check for + and -
            
                int v = (b-'0');
                accumDecimalValue = (10*accumDecimalValue) + v;    
               // System.err.println(accumDecimalValue);
            }
            p++;
        }                 
        
    }

    @Override
    public void closeRecord(int startPos) {
        
        messageTypes.restToRecordStart();
        
        //move the pointer up to the next record?
        bytePosStartField = ringBuffer.addBytePos.value = bytePosActive;
        
        FASTRingBuffer.publishWrites(ringBuffer.headPos,ringBuffer.workingHeadPos);
         
        // ** fields are now at the end of the record so the template Id is known
        
        //write it
        //flush it
        
        //TODO: Should we flush here?
        //ring buffer data is given over to the encoder
        
        //TODO: compiled encoder will need to detect new catalog on the fly!
        //for now just throw the data away
        FASTRingBuffer.dump(ringBuffer);
        
    }

    @Override
    public void closeField() {
        //selecting the message type one field at at time as we move forward
        int fieldType = messageTypes.moveNextField2();
               
        
        
        switch (fieldType) {
            case TypeTrie.TYPE_NULL:
                //TODO: what optional types are available? what if there are two then follow the order.
            //   System.err.println("need to find a null");
                
                break;
            case TypeTrie.TYPE_UINT:                
                FASTRingBufferWriter.writeInt(ringBuffer, (int)accumDecimalValue);;  
                break;            
            case TypeTrie.TYPE_SINT:
                FASTRingBufferWriter.writeInt(ringBuffer, (int)accumDecimalValue);;  
                break;    
            case TypeTrie.TYPE_ULONG:
                FASTRingBufferWriter.writeLong(ringBuffer, accumDecimalValue);;  
                break;    
            case TypeTrie.TYPE_SLONG:
                FASTRingBufferWriter.writeLong(ringBuffer, accumDecimalValue);;  
                break;    
            case TypeTrie.TYPE_ASCII:
                FASTRingBufferWriter.finishWriteBytes(ringBuffer, 0); //TODO: What is the length?
                break;
            case TypeTrie.TYPE_BYTES:                
                FASTRingBufferWriter.finishWriteBytes(ringBuffer, 0); //TODO: What is the length?
                break;
            case TypeTrie.TYPE_DECIMAL:
                int exponent = 0; //TODO: how to detect exponent?
                long mantissa = 0; //TODO: before dot appented to after dot plus any space needed for exponent.
                FASTRingBufferWriter.writeDecimal(ringBuffer, exponent, mantissa);  
                break;
            
            default:
                throw new UnsupportedOperationException("Field was "+fieldType);
        }
        
        //TODO: this field type is only the simple type or null
        //TODO: we still do not know if its optional 
        
        
        //closing field so keep this new active position as the potential start for the next field
        bytePosStartField = bytePosActive;
        // ** write as we go close out the field

        aftetDot = false;
        beforeDotValue = 0;
        accumDecimalValue = 0;
        
        
    }

    @Override
    public void closeFrame() {        
    }


    @Override
    public void openFrame() {
        //get new catalog if is has been changed by the other visitor
        byte[] catBytes = messageTypes.getCatBytes();
        if (!Arrays.equals(this.catBytes, catBytes)) {
            this.catBytes = catBytes;        
            catalog = new TemplateCatalogConfig(catBytes);
            System.err.println("new catalog");            
            //TODO: check assumption that templateID 0 is the one for sending catalogs.
            
            //if any partial write of field data is in progress just throw it away because 
            //next frame will begin again from the start of the message.
                        
            //ignore any byte kept so far in this message
            bytePosStartField = bytePosActive = ringBuffer.addBytePos.value;
            FASTRingBuffer.abandonWrites(ringBuffer.headPos,ringBuffer.workingHeadPos);
            
            // Write new catalog to stream.
            FASTRingBufferWriter.writeInt(ringBuffer, CATALOG_TEMPLATE_ID);        
            FASTRingBufferWriter.writeBytes(ringBuffer, catBytes);               
            
        }        
    }

}
