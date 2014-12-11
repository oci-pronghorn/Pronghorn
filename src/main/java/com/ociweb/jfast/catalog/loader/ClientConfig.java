package com.ociweb.jfast.catalog.loader;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class ClientConfig {

    private short preableBytes;
    private int bytesLengthMax;
    private int bytesGap;
    
    private int rbPrimaryRingBits = 8;
    private int rbTextRingBits = 12;
    
    private static final int NONE = -1;
    private int catalogId = NONE;

    
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
        
        bytesLengthMax = PrimitiveReader.readIntegerUnsigned(reader);
        bytesGap = PrimitiveReader.readIntegerUnsigned(reader);
        
        rbPrimaryRingBits = PrimitiveReader.readIntegerUnsigned(reader);
        rbTextRingBits = PrimitiveReader.readIntegerUnsigned(reader);
        
        catalogId = PrimitiveReader.readIntegerSigned(reader);
        
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

        PrimitiveWriter.writeIntegerUnsigned(bytesLengthMax, writer);
        PrimitiveWriter.writeIntegerUnsigned(bytesGap, writer);
        
        PrimitiveWriter.writeIntegerUnsigned(rbPrimaryRingBits, writer);
        PrimitiveWriter.writeIntegerUnsigned(rbTextRingBits, writer);
        
        PrimitiveWriter.writeIntegerSigned(catalogId, writer);
        
        
        
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
    
    public void setBytes(int max, int gap) {
        this.bytesLengthMax = max;
        this.bytesGap = gap;
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

    public void setCatalogTemplateId(int id) {
        if (NONE == id) {
            throw new FASTException("Catalog template Id may not be: "+NONE);
        }
        catalogId = id;
    }
    
    public boolean hasCatalogTemplate() {
        return NONE!=catalogId;
    }
    
    public int getCatalogTemplateId() {
        return catalogId;
    }   
    
}
