package com.ociweb.jfast;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.catalog.extraction.Extractor;
import com.ociweb.jfast.catalog.extraction.FieldTypeVisitor;
import com.ociweb.jfast.catalog.extraction.RecordFieldExtractor;
import com.ociweb.jfast.catalog.extraction.RecordFieldValidator;
import com.ociweb.jfast.catalog.extraction.StreamingVisitor;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTInputReactor;
import com.ociweb.jfast.stream.FASTListener;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferConsumer;
import com.ociweb.jfast.stream.FASTRingBufferReader;

import static com.ociweb.jfast.stream.FASTRingBufferReader.*;

import com.ociweb.jfast.stream.RingBuffers;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

/**
 * Command line application for importing and exporting data
 * between comma delimited and FAST encoded formated.
 * 
 * @author Nathan Tippy
 *
 */
public class FASTUtil {  

	private static final String ARG_TEMPLATE = "template";
	private static final String ARG_FAST = "fast";
	private static final String ARG_CSV = "csv";
	private static final String ARG_TASK = "task";
	
	private static final String VALUE_ENCODE = "encode";
	private static final String VALUE_DECODE = "decode";
	
	//TODO: move into property file for easy configuration
	private static final int fieldDelimiter = (int)',';    
	private static final byte[] recordDelimiter = new byte[]{'\r','\n'};
	private static final int openQuote = (int)'"';        
	private static final int closeQuote = (int)'"';
	private static final int escape = Integer.MIN_VALUE; //not used at this time
    
	
    public static void main(String[] args) {        
    	
    	String templateFilePath = getValue(ARG_TEMPLATE,args,null);
    	String fastFilePath = getValue(ARG_FAST,args,null);
    	String csvFilePath = getValue(ARG_CSV,args,null);
    	String task = getValue(ARG_TASK,args,null);//encode or decode
    	
    	//validate that we have all the needed args
    	
    	if (null == templateFilePath ||
    		null == fastFilePath ||
    		null == csvFilePath ||
    		null == task) {
    		System.err.println("FASTUtil -template <templateFile> -fast <fastFile> -csv <csvFile> -task encode|decode");
    		System.exit(-1);
    		return;
    	}
    	
    	//NOTE:
    	//for all writes it will append or create if it is missing, never delete any files
    	
    	//task specific validation 
    	    	
    	FASTUtil instance = new FASTUtil();
    	if (VALUE_ENCODE.equalsIgnoreCase(task)) {
    		File csvFile = new File(csvFilePath);
    		if (!csvFile.exists()) {
    			System.err.println("csv file not found: "+csvFilePath);
    			System.exit(-1);
    			return;
    		}
    		
    		File templateFile = new File(templateFilePath);
    		
    		RecordFieldExtractor typeAccum = new RecordFieldExtractor(buildRecordValidator());   
    		if (!templateFile.exists()) {
    			instance.encodeAndBuildTemplate(templateFilePath, csvFile, templateFile, typeAccum); 
    		} else {
    			instance.encodeGivenTemplate(csvFile, templateFilePath, typeAccum);  
    		}
    	} 
    	if (VALUE_DECODE.equalsIgnoreCase(task)) {
    		
    		//TODO: BUILD OUT DECODER
    		
    	} 
    }


	private void encodeGivenTemplate(File csvFile, String templateFile,
			RecordFieldExtractor typeAccum) {
		
		ClientConfig clientConfig = new ClientConfig();
		typeAccum.loadTemplate(templateFile, clientConfig);		
		StreamingVisitor visitor = new StreamingVisitor(typeAccum);
		
		try {
			FileChannel fileChannel = new RandomAccessFile(csvFile, "r").getChannel();                	
			Extractor ex = new Extractor(fieldDelimiter, recordDelimiter, openQuote, closeQuote, escape, 29);
		    ex.extract(fileChannel, visitor);  

		    //write out the new final template that was used at the end of the file.
		    String catalog = typeAccum.buildCatalog(true);
		    FileOutputStream fost = new FileOutputStream(templateFile);
		    fost.write(catalog.getBytes());
		    fost.close();
		    
		    
		} catch (IOException e) {
		    System.err.println(e.getLocalizedMessage());
		    System.exit(-2);
		}
	}



	private void encodeAndBuildTemplate(String templateFilePath,
			File csvFile, File templateFile, RecordFieldExtractor typeAccum) {
		System.out.println("No template provided so one will be generated at: "+templateFilePath);
		//pass over file generating the template as we pass over it			
		FieldTypeVisitor visitor1 = new FieldTypeVisitor(typeAccum);
		StreamingVisitor visitor2 = new StreamingVisitor(typeAccum);    			
		
		try {
			FileChannel fileChannel = new RandomAccessFile(csvFile, "r").getChannel();                	
			Extractor ex = new Extractor(fieldDelimiter, recordDelimiter, openQuote, closeQuote, escape, 29);
		    ex.extract(fileChannel, visitor1, visitor2);  

		    //write out the new final template that was used at the end of the file.
		    String catalog = typeAccum.buildCatalog(true);
		    FileOutputStream fost = new FileOutputStream(templateFile);
		    fost.write(catalog.getBytes());
		    fost.close();
		    
		    
		} catch (IOException e) {
		    System.err.println(e.getLocalizedMessage());
		    System.exit(-2);
		}
	}


    
    /**
     * pull from command line and if its not found there then pull the system value.
     * if neither of these are found the default is returned.
     * @param key
     * @param args
     * @return
     */
    public static String getValue(String key, String[] args, String def) {
        boolean found = false;
        for(String arg:args) {
            if (found) {
                return arg;
            }
            found = arg.toUpperCase().endsWith(key.toUpperCase());
        }
        return System.getProperty(key,def);
    }
    
	private static RecordFieldValidator buildRecordValidator() {
		//TODO: load these rules from a property file?
		//TODO: valid column counts should be included?
		
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
    
    
}
