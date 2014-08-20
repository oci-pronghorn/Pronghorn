package com.ociweb.jfast.loader;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class ClientConfig {

    private short preableBytes;
    private int textLengthMax;
    private int textGap;
    private int bytesLengthMax;
    private int bytesGap;
    
    private int rbPrimaryRingBits = 10;
    private int rbTextRingBits = 8;

    
    public ClientConfig(int primaryRingBits, int textRingBits) {
        this.rbPrimaryRingBits = primaryRingBits;
        this.rbTextRingBits = textRingBits;
    }
    
    
    //TODO: B, must set extra spacers to be written between
    //TODO: B, must set extra spacers to skip on read
    //TODO: B, must store the ignore templates without having catalog.
    
    
    //names, ids
//    private byte[] ignoreFieldIds; //1 for skip or 0 for allow
//    private final int[] bufferMaps;
//    private int bufferMapCount;
    
//  (t1,t2,t3)(t4,t5)t6  this is a set of sets, how is it easist to define?
    //maps for the grouping of maps.   (templateId,mapId) and maxMapId
    //private 
    public ClientConfig() {
//        bufferMaps = new int[templatIdsCount<<1];
//        bufferMapCount = 1;
//        ignoreFieldIds = new byte[scriptLength];
    }

    public ClientConfig(PrimitiveReader reader) {
        
        preableBytes = (short)PrimitiveReader.readIntegerUnsigned(reader);
        
        textLengthMax = PrimitiveReader.readIntegerUnsigned(reader);
        textGap = PrimitiveReader.readIntegerUnsigned(reader);
        
        bytesLengthMax = PrimitiveReader.readIntegerUnsigned(reader);
        bytesGap = PrimitiveReader.readIntegerUnsigned(reader);
        
        rbPrimaryRingBits = PrimitiveReader.readIntegerUnsigned(reader);
        rbTextRingBits = PrimitiveReader.readIntegerUnsigned(reader);
        
//        //read the filter fields list
//        int scriptLength = PrimitiveReader.readIntegerUnsigned(reader);
//        ignoreFieldIds = new byte[scriptLength];
//        PrimitiveReader.openPMap(scriptLength, reader);
//        int i = scriptLength;
//        while (--i>=0) {
//            ignoreFieldIds[i]= (byte)PrimitiveReader.popPMapBit(reader);
//        }
//        PrimitiveReader.closePMap(reader);
//        
//        //read the bufferMaps
//        bufferMapCount = PrimitiveReader.readIntegerUnsigned(reader);
//        int bufferSize = PrimitiveReader.readIntegerUnsigned(reader);
//        assert((bufferSize&1)==0);
//        bufferMaps = new int[bufferSize];
//        i = bufferSize;
//        while (--i>=0) {
//            bufferMaps[i]=PrimitiveReader.readIntegerUnsigned(reader);
//        }  
    }

    public void save(PrimitiveWriter writer) {
        
        PrimitiveWriter.writeIntegerUnsigned(preableBytes, writer);

        PrimitiveWriter.writeIntegerUnsigned(textLengthMax, writer);
        PrimitiveWriter.writeIntegerUnsigned(textGap, writer);

        PrimitiveWriter.writeIntegerUnsigned(bytesLengthMax, writer);
        PrimitiveWriter.writeIntegerUnsigned(bytesGap, writer);
        
        PrimitiveWriter.writeIntegerUnsigned(rbPrimaryRingBits, writer);
        PrimitiveWriter.writeIntegerUnsigned(rbTextRingBits, writer);
        
        
//        //write filter fields list
//        writer.writeIntegerUnsigned(ignoreFieldIds.length);
//        writer.openPMap(ignoreFieldIds.length);
//        int i = ignoreFieldIds.length;
//        while (--i>=0) {
//            PrimitiveWriter.writePMapBit(ignoreFieldIds[i], writer);
//        }
//        writer.closePMap();
//        
//        //write the bufferMaps
//        writer.writeIntegerUnsigned(bufferMapCount);
//        i = bufferMaps.length;
//        writer.writeIntegerUnsigned(i);
//        while (--i>=0) {
//            writer.writeIntegerUnsigned(bufferMaps[i]);
//        }
        
        
    }
    
    public short getPreableBytes() {
        return preableBytes;
    }

    public void setPreableBytes(short preableBytes) {
        this.preableBytes = preableBytes;
    }
    
    public void setText(int max, int gap) {
        this.textLengthMax = max;
        this.textGap = gap;
    }
    
    public void setBytes(int max, int gap) {
        this.bytesLengthMax = max;
        this.bytesGap = gap;
    }    
    
    public int getTextLength() {
        return this.textLengthMax;
    }
    
    public int getTextGap() {
        return this.textGap;
    }
    
    public int getBytesLength() {
        return this.bytesLengthMax;
    }
    
    public int getBytesGap() {
        return this.bytesGap;
    }

    public int getPrimaryRingBits() {
        return rbPrimaryRingBits;
    }

    public int getTextRingBits() {
        return rbTextRingBits;
    }


    
    
    
}
