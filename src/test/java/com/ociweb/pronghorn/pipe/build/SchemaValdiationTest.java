package com.ociweb.pronghorn.pipe.build;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;


public class SchemaValdiationTest {

	@Test
	public void fileNotFoundTest() {		
		    PrintStream temp = System.out;
			PrintStream temp2 = System.err;
			try {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					System.setOut(new PrintStream(baos));
					System.setErr(new PrintStream(new ByteArrayOutputStream()));			
					assertFalse(FROMValidation.testForMatchingFROMs("/NetSOMETHINGPayload.xml", BlankSchemaExample.class));
											
			} finally {
				System.setOut(temp);
				System.setErr(temp2);
			}
	}
	
	@Test
	public void sometestA() {
		
		PrintStream temp = System.out;
		PrintStream temp2 = System.err;
		String actual;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			System.setOut(new PrintStream(baos));
			System.setErr(new PrintStream(new ByteArrayOutputStream()));
			assertFalse(FROMValidation.testForMatchingFROMs("/rawDataSchema.xml", BlankSchemaExample.class));

			actual = new String(baos.toByteArray());
			assertTrue(actual.contains("public static final BlankSchemaExample instance = new BlankSchemaExample();"));
			assertTrue(actual.contains("public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager("));
			assertTrue(actual.contains("rawDataSchema.xml"));
			assertTrue(actual.contains("super(FROM);")); //constructor insides
			
			
		} finally {
			System.setOut(temp);
			System.setErr(temp2);
		}
		
		//System.out.println(actual);
	}
		
	@Test
	public void sometestB() {
		PrintStream temp = System.out;
		PrintStream temp2 = System.err;
		String actual;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			System.setOut(new PrintStream(baos));
			System.setErr(new PrintStream(new ByteArrayOutputStream()));
			assertFalse(	FROMValidation.checkSchema("/rawDataSchema.xml", BlankSchemaExample2.class) );
			actual = new String(baos.toByteArray());
			
			assertTrue(actual.contains("public static final int MSG_"));
			assertTrue(actual.contains("PipeReader.getMsgIdx"));
						
		} finally {
			System.setOut(temp);
			System.setErr(temp2);
		}
		
		//System.out.println(actual);
	}
	
}
