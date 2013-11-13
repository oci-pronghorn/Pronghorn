package com.ociweb.jfast.field;

public final class TypeMask {

	//need 6 bits total for type mask
	//each group of "similar" types must stay together as a block.
	
	public final static int IntegerUnSigned			= 0x00;//0000   even
	public final static int IntegerUnSignedOptional	= 0x01;//0001   odd for optional
	public final static int IntegerSigned           	= 0x02;//0010   even
	public final static int IntegerSignedOptional		= 0x03;//0011   odd
	
	public final static int LongUnSigned				= 0x04;//0100
	public final static int LongUnSignedOptional		= 0x05;//0101
	public final static int LongSigned				= 0x06;//0110
	public final static int LongSignedOptional		= 0x07;//0111
	
	public final static int TextASCII					= 0x08;//1000
	public final static int TextASCIIOptional			= 0x09;//1001
	public final static int TextUTF8					= 0x0A;//1010
	public final static int TextUTF8Optional			= 0x0B;//1011
	
	public final static int ByteArray					= 0x0C;//1100
	public final static int ByteArrayOptional  		= 0x0D;//1101
	
	public final static int Decimal		        	= 0x0E;//1110
	public final static int DecimalOptional			= 0x0F;//1111
	
	//room for next revision (booleans/enums)
	
}
