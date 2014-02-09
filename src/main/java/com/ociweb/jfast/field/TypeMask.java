//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

public final class TypeMask {

	//need 6 bits total for type mask
	//each group of "similar" types must stay together as a block.
	
	public final static int IntegerUnsigned			= 0x00;//00000   even
	public final static int IntegerUnsignedOptional	= 0x01;//00001   odd for optional
	public final static int IntegerSigned           	= 0x02;//00010   even
	public final static int IntegerSignedOptional		= 0x03;//00011   odd
	
	public final static int LongUnsigned				= 0x04;//00100
	public final static int LongUnsignedOptional		= 0x05;//00101
	public final static int LongSigned				= 0x06;//00110
	public final static int LongSignedOptional		= 0x07;//00111
	
	public final static int TextASCII					= 0x08;//01000
	public final static int TextASCIIOptional			= 0x09;//01001
	public final static int TextUTF8					= 0x0A;//01010
	public final static int TextUTF8Optional			= 0x0B;//01011
	
	//TODO: what if single/twin is an artifact of the parser only then
	//here we just mege the operation type of exponent into the type
	//copy, delta, default, const, none, shared 6 values? and one token?
	//if we define all the decimals first they can use the same index.
	
	public final static int Decimal        			= 0x0C;//01100
	public final static int DecimalOptional       	= 0x0D;//01101
	public final static int ByteArray	        		= 0x0E;//01110
	public final static int ByteArrayOptional			= 0x0F;//01111
	
	public final static int GroupSimple              = 0x10;//10000
	public final static int GroupTemplated           = 0x11;//10001
	
	//lots of room for the next revision, eg booleans and enums
	
	//special flag used internally by FASTDynamic* to know when to return control back to the caller.
	public final static int Stop                     = 0x1F;//11111

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
		switch (typeMask) {
			case IntegerUnsigned:
				return "IntegerUnsigned:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case IntegerUnsignedOptional:
				return "IntegerUnsignedOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case IntegerSigned:
				return "IntegerSigned:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case IntegerSignedOptional:
				return "IntegerSignedOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case LongUnsigned:
				return "LongUnsigned:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case LongUnsignedOptional:
				return "LongUnsignedOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case LongSigned:
				return "LongSigned:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case LongSignedOptional:
				return "LongSignedOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case TextASCII:
				return "TextASCII:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case TextASCIIOptional:
				return "TextASCIIOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case TextUTF8:
				return "TextUTF8:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case TextUTF8Optional:
				return "TextUTF8Optional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case Decimal:
				return "Decimal:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case DecimalOptional:
				return "DecimalOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case ByteArray:
				return "ByteArray:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			case ByteArrayOptional:
				return "ByteArrayOptional:"+prefix(6,'0',Integer.toBinaryString(typeMask));
			default:
				return "unknown type:"+prefix(6,'0',Integer.toBinaryString(typeMask));
		}
		
	}
	
}
