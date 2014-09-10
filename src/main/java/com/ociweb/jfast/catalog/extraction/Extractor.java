package com.ociweb.jfast.catalog.extraction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Extractor {

    private final int fieldDelimiter;
    private final byte[] recordDelimiter;
    private final int openQuote;
    private final int closeQuote;
    private final int escape;
    
    private final long BLOCK_SIZE = 1l<<28;//.25GB so 26 cycles  //26; //64MB
    
    //state while parsing
    boolean inQuote = false;
    boolean inEscape = false;
    int     contentPos = -1;
    boolean contentQuoted = false;
    final int tailPadding;  //padding required to ensure full length of tokens are not split across mapped blocks

    int recordStart = 0;
    
    
    final byte[] temp = "Astec Industries,".getBytes();
//    example for :1552 length 58
//
//    INDFY,Indofood Agri Resources,,56.1,56.15,56.1,56.15,400
//    example for :1584 length 23
//
//    GREE,,,0.0,0.0,0.0,,0
//    example for :1696 length 38
//
//    NNNLL,637417601,,0.0,0.0,0.0,24.98,0
//    example for :1728 length 40

    
    
    //TODO: B, Based on this design build another that can parse JSON
    
    //Parsing order of priority
    //  1.  escape
    //  2.  quotes
    //  3.  record delimiter
    //  4.  field delimiter
    //  5.  data
    
    //zero copy and garbage free
    //visitor may do copy and may produce garbage

    public Extractor(int fieldDelimiter, byte[] recordDelimiter,
                     int openQuote, int closeQuote, int escape) {
        this.fieldDelimiter = fieldDelimiter;
        this.recordDelimiter = recordDelimiter;
        this.openQuote = openQuote;
        this.closeQuote = closeQuote;
        this.escape = escape;
        
        this.tailPadding =   Math.max(
                                recordDelimiter.length,
                                temp.length);
    }

    
    public void extract(FileChannel fileChannel, ExtractionVisitor visitor) throws IOException {
        MappedByteBuffer mappedBuffer;
        
        long fileSize = fileChannel.size();
        long position = 0;
        
        
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        int padding = tailPadding;
        do {
            if (mappedBuffer.limit()+position==fileSize) {
                padding = 0;
            }
            
            visitor.openFrame();
            do {
                parse(mappedBuffer, visitor);
            } while (mappedBuffer.remaining()>padding);
            //notify the visitor that the buffer is probably going to change out from under them
            visitor.closeFrame();
            //only increment by exactly how many bytes were read assuming we started at zero
            position+=mappedBuffer.position();
                        
            if (true) { //hack to exit early
                break;
            }
            
            recordStart = 0;
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        } while (position<fileSize);
                
        if (flushContent(mappedBuffer,visitor)) {
            flushField(visitor);
            flushRecord(visitor, mappedBuffer.position());
        }
        
        
    }
    
    public void extract(FileChannel fileChannel, ExtractionVisitor visitor1, ExtractionVisitor visitor2) throws IOException {
        MappedByteBuffer mappedBuffer;
        
        long fileSize = fileChannel.size();
        long position = 0;
        
        
        mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        int padding = tailPadding;
        do {
            //the last go round must never use any padding, this padding is only needed when spanning two blocks.
            if (mappedBuffer.limit()+position==fileSize) {
                padding = 0;
            }
            
            visitor1.openFrame();
            do {
                parse(mappedBuffer, visitor1);
            } while (mappedBuffer.remaining()>padding);
            //notify the visitor that the buffer is probably going to change out from under them
            visitor1.closeFrame();            
            if (position+mappedBuffer.position()>=fileSize) {
                if (flushContent(mappedBuffer,visitor1)) {
                    flushField(visitor1);
                    flushRecord(visitor1, mappedBuffer.position());
                }
            }
            
            
            //TODO: each visitor needs their own state vars to cover any values crossing over the mapped boundry
            
            //visit second visitor while this block is still mapped
            mappedBuffer.position(0);
            
            visitor2.openFrame();            
            do {
                parse(mappedBuffer, visitor2);
            } while (mappedBuffer.remaining()>padding);
            //notify the visitor that the buffer is probably going to change out from under them
            visitor2.closeFrame();
            if (position+mappedBuffer.position()>=fileSize) {
                if (flushContent(mappedBuffer,visitor2)) {
                    flushField(visitor2);
                    flushRecord(visitor2, mappedBuffer.position());
                }
            }
                        
            
            //only increment by exactly how many bytes were read assuming we started at zero
            position+=mappedBuffer.position();
            
           
            
            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, Math.min(BLOCK_SIZE, fileSize-position));
        } while (position<fileSize);
                
    }
    
    //TODO: When building JSON parser the field names will also be extracted. These names will be written one after the other into a buffer
    //      once the end if the message is reached this full string is used as an additional information point to distinquish between 
    //      messages that have the same field signatures but define them with different.  Each unique set of labels will need to define
    //      its own TypeTrie
    
    private boolean flushContent(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (contentPos>=0 && mappedBuffer.position()>contentPos) {
            visitor.appendContent(mappedBuffer, contentPos, mappedBuffer.position(), contentQuoted);
            contentPos = -1;
            contentQuoted = false;
            return true;
        }
        return false;
    }

    private void flushRecord(ExtractionVisitor visitor, int pos) {
        
        
       visitor.closeRecord(recordStart);
       
       recordStart = pos+recordDelimiter.length;
       
    }

    private void flushField(ExtractionVisitor visitor) {
        visitor.closeField();
    }


    private void parse(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        
//        if (foundHere(mappedBuffer,temp)) {
//            contentPos = mappedBuffer.position();//hack test.
//            mappedBuffer.position(mappedBuffer.position()+temp.length);
//            
//        } else {
//        
            parseEscape(mappedBuffer, visitor);
     //   }
    }


    private void parseEscape(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (mappedBuffer.get(mappedBuffer.position())==escape) { 
            if (inEscape) {
                //starts new content block from this location
                contentPos = mappedBuffer.position();
                contentQuoted = inQuote;
                inEscape = false;
            } else {
                flushContent(mappedBuffer, visitor);                
                inEscape = true;
            }
            mappedBuffer.position(mappedBuffer.position()+1);
        } else {
            parseQuote(mappedBuffer, visitor);
            inEscape = false;
        }
    }

    private void parseQuote(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (inQuote) {
            if (mappedBuffer.get(mappedBuffer.position())==closeQuote) {
                if (inEscape) {
                    //starts new content block from this location
                    contentPos = mappedBuffer.position();
                    contentQuoted = inQuote;
                    inEscape = false;
                } else {                                
                    inQuote = false;  
                }
                mappedBuffer.position(mappedBuffer.position()+1);
            } else {
                parseRecord(mappedBuffer, visitor);   
            }
            
            
        } else {
            if (mappedBuffer.get(mappedBuffer.position())==openQuote) {
                if (inEscape) {
                    //starts new content block from this location
                    contentPos = mappedBuffer.position();
                    contentQuoted = inQuote;
                    inEscape = false;
                } else {
                    inQuote = true;
                }
                mappedBuffer.position(mappedBuffer.position()+1);
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
                    flushRecord(visitor, mappedBuffer.position());
                }
            }
            mappedBuffer.position(mappedBuffer.position()+recordDelimiter.length);
        } else {
            parseField(mappedBuffer, visitor);       
            
        }           
    }
   

    private void parseField(MappedByteBuffer mappedBuffer, ExtractionVisitor visitor) {
        if (mappedBuffer.get(mappedBuffer.position())==fieldDelimiter) {
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
            mappedBuffer.position(mappedBuffer.position()+1);
        } else {
            parseContent(mappedBuffer); 
        }      
    }   
    
    private void parseContent(MappedByteBuffer mappedBuffer) {        
        if (contentPos<0) {
            contentPos = mappedBuffer.position();
            contentQuoted = inQuote;
        }
        
        //check for special text that starts out ok but then goes wrong.
          if (foundHere(mappedBuffer,temp)) {
              
              //contentPos = mappedBuffer.position();//hack test.
              mappedBuffer.position(mappedBuffer.position()+temp.length);
          
          } else {
            
            mappedBuffer.position(mappedBuffer.position()+1);
          }
    }  
    

    private boolean foundHere(MappedByteBuffer data, byte[] goal) {
        
        int i = goal.length;
        if (data.position()+i>data.limit()) {
            return false;
        }
        while (--i>=0) {
            if (data.get(data.position()+i)!=goal[i]) {
                return false;
            }
        }
        return true;
    }
        
    
    
}
