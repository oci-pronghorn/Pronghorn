package com.ociweb.jfast.catalog.extraction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

public class ExtractorTest {

    @Test
    public void extractTest() throws FileNotFoundException {
        
        ByteBuffer fieldDelimiter = ByteBuffer.allocate(1);
        fieldDelimiter.put((byte)',');
        fieldDelimiter.flip();
                
        ByteBuffer recordDelimiter = ByteBuffer.allocate(2);
        recordDelimiter.put((byte)'\n');
        recordDelimiter.flip();
        
        ByteBuffer openQuote = ByteBuffer.allocate(1);
        openQuote.put((byte)'"');
        openQuote.flip();
        
        ByteBuffer closeQuote = ByteBuffer.allocate(1);
        closeQuote.put((byte)'"');
        closeQuote.flip();
        
        ByteBuffer escape = ByteBuffer.allocate(1);
        escape.put((byte)'/'); 
        escape.flip();
        
        String fullPath = "/home/nate/flat/example.txt";
        
        ExtractionVisitor visitor = new ExtractionVisitor() {
            
            @Override
            public void contextSwitch() {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public void closeRecord() {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public void closeField() {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public void appendContent(MappedByteBuffer mappedBuffer, int start, int limit, boolean contentQuoted) {
                
                //hack test for now.
                
                byte[] target = new byte[limit-start];
                ByteBuffer dup = mappedBuffer.duplicate();
                dup.position(start);
                dup.limit(limit);
                dup.get(target,0,limit-start);

                
             //   System.err.println(start+" to "+limit+" "+new String(target));

               
            }
        };
                
        if (null!=fullPath && fullPath.length()>0) {
            File file = new File(fullPath);
            if (file.exists()) {
                FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
                
                //TODO: add 1 pass to extract the types using the map reduce approach by counting chars
                //We could generate a template from the type data?
                //TODO: add 1 pass to map data directly to field types and put in ring buffer for usage.
                
                
                Extractor ex = new Extractor(fieldDelimiter, recordDelimiter, openQuote, closeQuote, escape);
                
                try {
                    ex.extract(fileChannel, visitor);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
            }
        
        
        }
    }
    
}
