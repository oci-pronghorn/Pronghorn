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

import com.ociweb.jfast.stream.FASTRingBuffer;

public class ExtractorTest {

    @Test
    public void extractTest() throws FileNotFoundException {
        
      //  FieldTypeVisitor visitor = new FieldTypeVisitor();
        
        int fieldDelimiter = (int)',';
                
        byte[] recordDelimiter = new byte[]{'\n'};
        
        int openQuote = (int)'"';        
        int closeQuote = (int)'"';
        
        int escape = (int)'/';
        
        String fullPath = "/home/nate/flat/example.txt";
        
        ExtractionVisitor visitor = new ExtractionVisitor() {
            
            @Override
            public void closeFrame() {
                // TODO Auto-generated method stub
                
            }
            
            @Override
            public void closeRecord(int startPos) {
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

                //add a test here
                
             //   System.err.println(start+" to "+limit+" "+new String(target));

               
            }

            @Override
            public void openFrame() {
                // TODO Auto-generated method stub
                
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
    
    @Test
    public void fieldTypeExtractionTest() throws FileNotFoundException {
        
        
        int fieldDelimiter = (int)',';
                
        byte[] recordDelimiter = new byte[]{'\r','\n'};
        
        int openQuote = (int)'"';        
        int closeQuote = (int)'"';
        
        //Not using escape in this test file
        int escape = Integer.MIN_VALUE;
        
        String fullPath = "/home/nate/flat/fullExample.txt";
      //  String fullPath = "/home/nate/flat/example.txt";
         
        FieldTypeVisitor visitor = new FieldTypeVisitor();
                
        if (null!=fullPath && fullPath.length()>0) {
            File file = new File(fullPath);
            if (file.exists()) {
                FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
                
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
    
    @Test
    public void dataExtractionTest() throws FileNotFoundException {
        
        
        int fieldDelimiter = (int)',';
                
        byte[] recordDelimiter = new byte[]{'\r','\n'};
        
        int openQuote = (int)'"';        
        int closeQuote = (int)'"';
        
        //Not using escape in this test file
        int escape = Integer.MIN_VALUE;
        
        String fullPath = "/home/nate/flat/fullExample.txt";
      //  String fullPath = "/home/nate/flat/example.txt";
         
        FieldTypeVisitor visitor1 = new FieldTypeVisitor(); //TODO: must use stream to write all the data to FAST data file.
       
        FASTRingBuffer ringBuffer = new FASTRingBuffer((byte)20, (byte)24, null, null); //TODO: produce from catalog.
        StreamingVisitor visitor2 = new StreamingVisitor(visitor1.getTypes(), ringBuffer);
        
                
        if (null!=fullPath && fullPath.length()>0) {
            File file = new File(fullPath);
            if (file.exists()) {
                FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
                
                Extractor ex = new Extractor(fieldDelimiter, recordDelimiter, openQuote, closeQuote, escape);
                
                try {
                    ex.extract(fileChannel, visitor1, visitor2);  

                   // ex.extract(fileChannel, visitor1); 
                    
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }                
            }            
        }
        
        
    }
    
    
}
