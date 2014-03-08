package com.ociweb.jfast.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.loader.TemplateLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTDynamicReader;

public class Complex30000Benchmark extends Benchmark {

	FASTInputByteArray fastInput;
	PrimitiveReader primitiveReader;
	FASTDynamicReader dynamicReader;
	
	public Complex30000Benchmark() {
		FASTInput templateCatalogInput = new FASTInputByteArray(buildRawCatalogData());
		TemplateCatalog catalog = new TemplateCatalog(new PrimitiveReader(templateCatalogInput));
		
		byte prefixSize = 4;
		catalog.setMessagePrefix(prefixSize);	
		
		//connect to file		
		URL sourceData = getClass().getResource("/performance/complex30000.dat");
		File fileSource = new File(sourceData.getFile());
			
		try {
			//do not want to time file access so copy file to memory
			byte[] fileData = new byte[(int) fileSource.length()];
			FileInputStream inputStream = new FileInputStream(fileSource);
			int readBytes = inputStream.read(fileData);
			inputStream.close();
			assertEquals(fileData.length,readBytes);
			
			fastInput = new FASTInputByteArray(fileData);
			primitiveReader = new PrimitiveReader(fastInput);
			dynamicReader = new FASTDynamicReader(primitiveReader, catalog);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private byte[] buildRawCatalogData() {
		URL source = getClass().getResource("/performance/example.xml");
			
		
		ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
		File fileSource = new File(source.getFile());
		try {			
			TemplateLoader.buildCatalog(catalogBuffer, fileSource);
		} catch (ParserConfigurationException | SAXException | IOException e) {
			e.printStackTrace();
		}
		
		assertTrue("Catalog must be built.",catalogBuffer.size()>0);
		
		byte[] catalogByteArray = catalogBuffer.toByteArray();
		return catalogByteArray;
	}
	
	public static void main(String[] args) {
		System.err.println("loading...");
		Complex30000Benchmark obj = new Complex30000Benchmark();
		System.err.println("testing...");
		obj.timeDecodeComplex30000(1);
		System.err.println("done");
	}
	
	public int timeDecodeComplex30000(int reps) {

		    int result = 0;
			while (--reps>=0) {

				int data = 0;
				while (0!=(data = dynamicReader.hasMore())) {
					//This is where we will do the Encode for the other benchmark
					result |= data;
				}
				
				fastInput.reset();
				primitiveReader.reset();
				dynamicReader.reset();
				
			}
			return result;
		
	}
}
