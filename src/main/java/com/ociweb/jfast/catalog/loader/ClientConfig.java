package com.ociweb.jfast.catalog.loader;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class ClientConfig {

    private short preambleBytes;
    
    private int bytesLengthMax = 4096;
    private int bytesGap = 64;
    
    private int rbPrimaryRingBits = 7;
    private int rbTextRingBits = 15;
    
    private static final int NONE = -1;
    private int catalogId = NONE;

    //TODO: AA,  these will be injected at runtime and should not be part of the saved cat bytes?
    
    public ClientConfig(int primaryRingBits, int textRingBits) {
        this.rbPrimaryRingBits = primaryRingBits;
        this.rbTextRingBits = textRingBits;
    }
    
    public ClientConfig(int primaryRingBits, int textRingBits, int bytesLength, int bytesGap) {
        this.rbPrimaryRingBits = primaryRingBits;
        this.rbTextRingBits = textRingBits;
        this.bytesLengthMax = bytesLength;
        this.bytesGap = bytesGap;
        
    }
    
    public ClientConfig() {
    }

    public ClientConfig(PrimitiveReader reader) {
        
        preambleBytes = (short)PrimitiveReader.readIntegerUnsigned(reader);
        
        bytesLengthMax = PrimitiveReader.readIntegerUnsigned(reader);
        bytesGap = PrimitiveReader.readIntegerUnsigned(reader);
        
        rbPrimaryRingBits = PrimitiveReader.readIntegerUnsigned(reader);
        rbTextRingBits = PrimitiveReader.readIntegerUnsigned(reader);
        
        catalogId = PrimitiveReader.readIntegerSigned(reader);

    }

    public void save(PrimitiveWriter writer) {
        
        PrimitiveWriter.writeIntegerUnsigned(preambleBytes, writer);

        PrimitiveWriter.writeIntegerUnsigned(bytesLengthMax, writer);
        PrimitiveWriter.writeIntegerUnsigned(bytesGap, writer);
        
        PrimitiveWriter.writeIntegerUnsigned(rbPrimaryRingBits, writer);
        PrimitiveWriter.writeIntegerUnsigned(rbTextRingBits, writer);
        
        PrimitiveWriter.writeIntegerSigned(catalogId, writer);
    
        
    }
    
    public short getPreableBytes() {
        return preambleBytes;
    }

    public void setPreableBytes(short preableBytes) {
        this.preambleBytes = preableBytes;
    }
    
    public int getBytesLength() {
        return this.bytesLengthMax;
    }
    
    public int getBytesGap() {
        return this.bytesGap;
    }

    public void setCatalogTemplateId(int id) {
        if (NONE == id) {
            throw new FASTException("Catalog template Id may not be: "+NONE);
        }
        catalogId = id;
    }

}
