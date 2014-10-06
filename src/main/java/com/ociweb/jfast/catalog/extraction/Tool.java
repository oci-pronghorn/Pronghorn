package com.ociweb.jfast.catalog.extraction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.ociweb.jfast.stream.FASTRingBuffer;

public class Tool {

	//TODO: this is just an experimental class the core of this work must be moved over to FASTUtil
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Tool tool = new Tool();
		
		try {
			//tool.fieldTypeExtractionTest("/home/nate/flat/fullExample.txt");
			tool.dataExtractionTest("/home/nate/flat/fullExample.txt");
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		
		
	}
	
    public void fieldTypeExtractionTest(String fullPath) throws FileNotFoundException {
        
        
        int fieldDelimiter = (int)',';
                
        byte[] recordDelimiter = new byte[]{'\r','\n'};
        
        int openQuote = (int)'"';        
        int closeQuote = (int)'"';
        
        //Not using escape in this test file
        int escape = Integer.MIN_VALUE;
        
         
        FieldTypeVisitor visitor = new FieldTypeVisitor( new RecordFieldExtractor(buildRecordValidator()) );
              
        //TODO: should not be part of unit tests, need to make an app for testing files and unit test should check smaller structure.   
        
        if (null!=fullPath && fullPath.length()>0) {
            File file = new File(fullPath);
            if (file.exists()) {
                FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
                
                CSVTokenizer ex = new CSVTokenizer(fieldDelimiter, recordDelimiter, openQuote, closeQuote, escape, 29);
                
                try {
                    ex.extract(fileChannel, visitor);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }                
            }            
        }
    }

	private RecordFieldValidator buildRecordValidator() {
		return new RecordFieldValidator() {

			@Override
			public boolean isValid(int fieldCount, int nullCount, int utf8Count, int asciiCount, int firstFieldLength, int firstField) {
				
		        return nullCount<=2 &&        //Too much missing data, 
		              utf8Count==0 &&        //data known to be ASCII so this is corrupted
		              (asciiCount==0 || asciiCount==2) && //only two known configurations for ascii  
		              firstFieldLength<=15 && //key must not be too large
		              firstField!=RecordFieldExtractor.TYPE_NULL; //known primary key is missing

			}
			
		};
		
		
	}
    
    public void dataExtractionTest(String fullPath) throws FileNotFoundException {
        
        
        int fieldDelimiter = (int)',';
                
        byte[] recordDelimiter = new byte[]{'\r','\n'};
        
        int openQuote = (int)'"';        
        int closeQuote = (int)'"';
        
        //Not using escape in this test file
        int escape = Integer.MIN_VALUE;
                 
        RecordFieldExtractor typeAccum = new RecordFieldExtractor(buildRecordValidator());   
        
        
        FieldTypeVisitor visitor1 = new FieldTypeVisitor(typeAccum); //TODO: must use stream to write all the data to FAST data file.
       
        StreamingVisitor visitor2 = new StreamingVisitor(typeAccum);
        
        //TODO: should not be part of unit tests, need to make an app for testing files and unit test should check smaller structure.   
        
        if (null!=fullPath && fullPath.length()>0) {
            File file = new File(fullPath);
            if (file.exists()) {
                FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
                
                CSVTokenizer ex = new CSVTokenizer(fieldDelimiter, recordDelimiter, openQuote, closeQuote, escape, 29);
                
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
