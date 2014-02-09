package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import org.junit.Test;

public class TypeMaskTest {

	
	@Test
	public void toStringTest() {
		
		assertEquals(              "ByteArray:001110",TypeMask.toString(TypeMask.ByteArray));
		assertEquals(      "ByteArrayOptional:001111",TypeMask.toString(TypeMask.ByteArrayOptional));
		
		assertEquals(                "Decimal:001100",TypeMask.toString(TypeMask.Decimal));
		assertEquals(        "DecimalOptional:001101",TypeMask.toString(TypeMask.DecimalOptional));
		
		assertEquals(          "IntegerSigned:000010",TypeMask.toString(TypeMask.IntegerSigned));
		assertEquals(  "IntegerSignedOptional:000011",TypeMask.toString(TypeMask.IntegerSignedOptional));
		
		assertEquals(        "IntegerUnsigned:000000",TypeMask.toString(TypeMask.IntegerUnsigned));
		assertEquals("IntegerUnsignedOptional:000001",TypeMask.toString(TypeMask.IntegerUnsignedOptional));
			
		assertEquals(             "LongSigned:000110",TypeMask.toString(TypeMask.LongSigned));
		assertEquals(     "LongSignedOptional:000111",TypeMask.toString(TypeMask.LongSignedOptional));
		
		assertEquals(           "LongUnsigned:000100",TypeMask.toString(TypeMask.LongUnsigned));
		assertEquals(   "LongUnsignedOptional:000101",TypeMask.toString(TypeMask.LongUnsignedOptional));
		
		assertEquals(              "TextASCII:001000",TypeMask.toString(TypeMask.TextASCII));
		assertEquals(      "TextASCIIOptional:001001",TypeMask.toString(TypeMask.TextASCIIOptional));
			
		assertEquals(              "TextUTF8:001010",TypeMask.toString(TypeMask.TextUTF8));
		assertEquals(      "TextUTF8Optional:001011",TypeMask.toString(TypeMask.TextUTF8Optional));
	}
	
}
