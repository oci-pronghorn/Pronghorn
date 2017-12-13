package com.ociweb.jpgRaster;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaTest {


	@Test
	public void SchemaOneSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/SchemaOne.xml", SchemaOneSchema.class));
	}
	
	@Test
	public void JPGScannerSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/JPGScanner.xml", JPGScannerSchema.class));
	}
	
	@Test
	public void HuffmanSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/Huffman.xml", HuffmanSchema.class));
	}
	
	@Test
	public void InverseQuantizerSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/InverseQuantizer.xml", InverseQuantizerSchema.class));
	}
	
	@Test
	public void InverseDCTSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/InverseDCT.xml", InverseDCTSchema.class));
	}
	
	@Test
	public void YCbCrToRGBSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/YCbCrToRGB.xml", YCbCrToRGBSchema.class));
	}
	
	@Test
	public void BMPSchemaFROMTest() {
		
		assertTrue(FROMValidation.checkSchema("/BMP.xml", BMPSchema.class));
	}
	
	
}
