//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

public final class TypeMask {

	//need 6 bits total for type mask
	//each group of "similar" types must stay together as a block.
	
	//1
	public final static int IntegerUnsigned			= 0x00;//00000   even
	public final static int IntegerUnsignedOptional	= 0x01;//00001   odd for optional
	public final static int IntegerSigned           	= 0x02;//00010   even
	public final static int IntegerSignedOptional		= 0x03;//00011   odd
	
	//2
	public final static int LongUnsigned				= 0x04;//00100
	public final static int LongUnsignedOptional		= 0x05;//00101
	public final static int LongSigned				= 0x06;//00110
	public final static int LongSignedOptional		= 0x07;//00111
	
	//2
	public final static int TextASCII					= 0x08;//01000
	public final static int TextASCIIOptional			= 0x09;//01001
	public final static int TextUTF8					= 0x0A;//01010
	public final static int TextUTF8Optional			= 0x0B;//01011
	
	//3
	public final static int Decimal        			= 0x0C;//01100
	public final static int DecimalOptional       	= 0x0D;//01101
	//2
	public final static int ByteArray	        		= 0x0E;//01110
	public final static int ByteArrayOptional			= 0x0F;//01111
		
	public final static int Group                    = 0x10;//10000
	//1
	public final static int GroupLength              = 0x14;//10100  //for sequence this is an uint32
	
	public final static int Dictionary               = 0x18;//11000
	
	//TODO: build unit test to confirm by reflection that every generated Token can be found
	
	//for code generation need to know the substring of the method related to this type.
	public final static String[] methodTypeName = new String[]{
		"IntegerUnsigned",
		"IntegerUnsigned",
		"IntegerSigned",
		"IntegerSigned",
		"LongUnsigned",
		"LongUnsigned",
		"LongSigned",
		"LongSigned",
		"ASCII",
		"ASCII",
		"UTF8",
		"UTF8",
		"Decimal", //need  exponent and mantissa strings.
		"Decimal",
		"Bytes",
		"Bytes",
		"Group",
		"Reserved1",
		"Reserved2",
		"Reserved3",
		"Length",
		"Reserved5",
		"Reserved6",
		"Reserved7",
		"Dictionary"
		};
	
	public final static String[] methodTypeInstanceName = new String[]{
		"readerInteger",
		"readerInteger",
		"readerInteger",
		"readerInteger",
		"readerLong",
		"readerLong",
		"readerLong",
		"readerLong",
		"readerText",
		"readerText",
		"readerText",
		"readerText",
		"readerDecimal", //need  exponent and mantissa strings.
		"readerDecimal",
		"readerBytes",
		"readerBytes",
		"this",
		"Reserved1",
		"Reserved2",
		"Reserved3",
		"this",
		"Reserved5",
		"Reserved6",
		"Reserved7",
		"this"
		};
	
	public final static String[] methodTypeSuffix = new String[]{
		"",
		"Optional",
		"",
		"Optional",
		"",
		"Optional",
		"",
		"Optional",
		"",
		"Optional",
		"",
		"Optional",
		"",
		"Optional",
		"",
		"Optional",
		"","","",
		"","","",
		"","",""
		};
	
	
	//lots of room for the next revision, eg booleans and enums
	
	//special flag used internally by FASTDynamic* to know when to return control back to the caller.
	//public final static int Stop                     = 0x1F;//11111

	private static String prefix(int len, char c, String value) {
		StringBuilder builder = new StringBuilder();
		int add = len -value.length();
		while (--add>=0) {
			builder.append(c);
		}
		builder.append(value);
		return builder.toString();
		
	}
	
	//This method for debugging and therefore can produce garbage.
	public static String toString(int typeMask) {
		
		return methodTypeName[typeMask]+
				methodTypeSuffix[typeMask]+
				":"+				
				prefix(6,'0',Integer.toBinaryString(typeMask));
		
	}
	
}
