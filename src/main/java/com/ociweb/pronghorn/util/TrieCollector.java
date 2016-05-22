package com.ociweb.pronghorn.util;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class TrieCollector {

    private final TrieParser trie;
    private final TrieParserReader reader;
    private int squenceCount = 0;
    
    private final static int MAX_TEXT_LENGTH = 1024;
    private final Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance,3,MAX_TEXT_LENGTH));
    private final int rawChunkSize = Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
    
    public TrieCollector(int size) {
        trie = new TrieParser(size, 2, false, false);
        reader = new TrieParserReader();
        pipe.initBuffers();        
    }
    
    public int valueOf(byte[] data, int offset, int length, int mask) {        
        int value = (int)TrieParserReader.query(reader, trie, data, offset, length, mask, -1);
        if (value>0) {
            return value;
        } else {
            trie.setValue(data, offset, length, mask, ++squenceCount);
            return squenceCount;
        } 
    }
    
    public int valueOfUTF8(CharSequence cs) {
        
        pipe.reset();
        Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        
        int origPos = Pipe.getBlobWorkingHeadPosition(pipe);
        int len = Pipe.copyUTF8ToByte(cs, 0, cs.length(), pipe);
        Pipe.addBytePosAndLen(pipe, origPos, len);        
        Pipe.publishWrites(pipe);
        Pipe.confirmLowLevelWrite(pipe, rawChunkSize);
        
        Pipe.takeMsgIdx(pipe);
        Pipe.confirmLowLevelRead(pipe, rawChunkSize);
       
        int meta = Pipe.takeRingByteMetaData(pipe);
        int result = valueOf(Pipe.byteBackingArray(meta, pipe),Pipe.bytePosition(meta, pipe, Pipe.takeRingByteLen(pipe)),Pipe.takeRingByteLen(pipe),Pipe.blobMask(pipe));
        
        Pipe.releaseReadLock(pipe);
        return result;
    }
    
    
}
