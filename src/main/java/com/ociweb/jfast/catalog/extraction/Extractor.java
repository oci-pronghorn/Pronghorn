package com.ociweb.jfast.catalog.extraction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Extractor {

    private final ByteBuffer fieldDelimiter;
    private final ByteBuffer recordDelimiter;
    private final ByteBuffer openQuote;
    private final ByteBuffer closeQuote;
    private final ByteBuffer escape;
    
    private final long BLOCK_SIZE = 1l<<20; //1MB
    
    //state while parsing
    boolean inQuote = false;
    boolean inEscape = false;
    int     contentPos = -1;
    boolean contentQuoted = false;
    final int tailPadding;  //padding required to ensure full length of tokens are not split across mapped blocks

    //TODO: B, Based on this design build another that can parse JSON
    
    //Parsing order of priority
    //  1.  escape
    //  2.  quotes
    //  3.  record delimiter
    //  4.  field delimiter
    //  5.  data
    
    //zero copy and garbage free
    //visitor may do copy and may produce garbage

    public Extractor(ByteBuffer fieldDelimiter, ByteBuffer recordDelimiter,
                     ByteBuffer openQuote, ByteBuffer closeQuote, ByteBuffer escape) {
        this.fieldDelimiter = fieldDelimiter;
        this.recordDelimiter = recordDelimiter;
        this.openQuote = openQuote;
        this.closeQuote = closeQuote;
        this.escape = escape;
        
        this.tailPadding = Math.max(
                              Math.max(
                                Math.max(fieldDelimiter.remaining(),recordDelimiter.remaining()),
                                Math.max(openQuote.remaining(),closeQuote.remaining())),
                              escape.remaining());
    }

    
    public void extract(FileChannel fileChannel, ExtractionVisitor visitor) throws IOException {
        MappedByteBuffer mappedBuffer;
        
        long fileSize = fileChannel.size();
        long position = 0;
        
        
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        
        do {
            do {
                parseEscape(mappedBuffer, visitor);
            } while (mappedBuffer.remaining()>tailPadding);
            //notify the visitor that the buffer is probably going to change out from under them
            visitor.frameSwitch();
            //only increment by exactly how many bytes were read assuming we started at zero
            position+=mappedBuffer.position();
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        } while (position<fileSize);
                
    }
    
    public void extract(FileChannel fileChannel, ExtractionVisitor visitor1, ExtractionVisitor visitor2) throws IOException {
        MappedByteBuffer mappedBuffer;
        
        long fileSize = fileChannel.size();
        long position = 0;
        
        
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        
        do {
            do {
                parseEscape(mappedBuffer, visitor1);
            } while (mappedBuffer.remaining()>tailPadding);
            //notify the visitor that the buffer is probably going to change out from under them
            visitor1.frameSwitch();
            
            //visit second visitor while this block is still mapped
            mappedBuffer.position(0);
            do {
                parseEscape(mappedBuffer, visitor2);
            } while (mappedBuffer.remaining()>tailPadding);
            //notify the visitor that the buffer is probably going to change out from under them
            visitor2.frameSwitch();
            
            
            
            //only increment by exactly how many bytes were read assuming we started at zero
            position+=mappedBuffer.position();
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        } while (position<fileSize);
                
    }
    
    private void flushContent(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (contentPos>=0 && mappedBuffer.position()>contentPos) {
            visitor.appendContent(mappedBuffer, contentPos, mappedBuffer.position(), contentQuoted);
            contentPos = -1;
            contentQuoted = false;
        }
    }

    private void flushRecord(ExtractionVisitor visitor) {
       visitor.closeRecord();
    }

    private void flushField(ExtractionVisitor visitor) {
        visitor.closeField();
    }
    

    private void parseEscape(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (foundHere(mappedBuffer, escape)) {
            if (inEscape) {
                //starts new content block from this location
                contentPos = mappedBuffer.position();
                contentQuoted = inQuote;
                inEscape = false;
            } else {
                flushContent(mappedBuffer, visitor);                
                inEscape = true;
            }
            mappedBuffer.position(mappedBuffer.position()+escape.remaining());
        } else {
            parseQuote(mappedBuffer, visitor);
            inEscape = false;
        }
    }

    private void parseQuote(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (inQuote) {
            if (foundHere(mappedBuffer,closeQuote)) {
                if (inEscape) {
                    //starts new content block from this location
                    contentPos = mappedBuffer.position();
                    contentQuoted = inQuote;
                    inEscape = false;
                } else {                                
                    inQuote = false;  
                }
                mappedBuffer.position(mappedBuffer.position()+closeQuote.remaining());
            } else {
                parseRecord(mappedBuffer, visitor);   
            }
            
            
        } else {
            if (foundHere(mappedBuffer,openQuote)) {
                if (inEscape) {
                    //starts new content block from this location
                    contentPos = mappedBuffer.position();
                    contentQuoted = inQuote;
                    inEscape = false;
                } else {
                    inQuote = true;
                }
                mappedBuffer.position(mappedBuffer.position()+openQuote.remaining());
            } else {
                parseRecord(mappedBuffer, visitor);       
                
            }           
            
        }
    }
    
    private void parseRecord(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (foundHere(mappedBuffer,recordDelimiter)) {
            if (inEscape) {
                //starts new content block from this location
                contentPos = mappedBuffer.position();
                contentQuoted = inQuote;
                inEscape = false;
            } else {
                if (inQuote) {
                    parseField(mappedBuffer, visitor);  
                } else {
                    flushContent(mappedBuffer, visitor);
                    flushField(visitor);
                    flushRecord(visitor);
                }
            }
            mappedBuffer.position(mappedBuffer.position()+recordDelimiter.remaining());
        } else {
            parseField(mappedBuffer, visitor);       
            
        }           
    }
   

    private void parseField(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (foundHere(mappedBuffer,fieldDelimiter)) {
            if (inEscape) {
                //starts new content block from this location
                contentPos = mappedBuffer.position();
                contentQuoted = inQuote;
                inEscape = false;
            } else {
                if (inQuote) {
                    parseContent(mappedBuffer); 
                } else {                
                    flushContent(mappedBuffer, visitor);
                    flushField(visitor);
                }
            }
            mappedBuffer.position(mappedBuffer.position()+fieldDelimiter.remaining());
        } else {
            parseContent(mappedBuffer); 
        }      
    }   
    
    private void parseContent(MappedByteBuffer mappedBuffer) {        
        if (contentPos<0) {
            contentPos = mappedBuffer.position();
            contentQuoted = inQuote;
        }
        mappedBuffer.position(mappedBuffer.position()+1);   
    }  
    

    private boolean foundHere(MappedByteBuffer data, ByteBuffer goal) {
        
        int i = goal.remaining();
        int dpos = data.position();
        int gpos = goal.position();
        if (i+dpos>data.limit()) {
            return false;
        }
        
        while (--i>=0) {
            if (data.get(dpos+i)!=goal.get(gpos+i)) {
                return false;
            }
            
        }
        return true;
    }
        
    
    
}
