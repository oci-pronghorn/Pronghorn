package com.ociweb.jfast.field;

public final class TypeMask {

	//need 6 bits total for type mask
	//each group of "similar" types must stay together as a block.
	
	public final static int IntegerUnSigned			= 0x00;//000000   even
	public final static int IntegerUnSignedOptional	= 0x01;//000001   odd for optional
	public final static int IntegerSigned           	= 0x02;//000010   even
	public final static int IntegerSignedOptional		= 0x03;//000011   odd
	
	public final static int LongUnSigned				= 0x04;//000100
	public final static int LongUnSignedOptional		= 0x05;//000101
	public final static int LongSigned				= 0x06;//000110
	public final static int LongSignedOptional		= 0x07;//000111
	
	public final static int TextASCII					= 0x08;//001000
	public final static int TextASCIIOptional			= 0x09;//001001
	public final static int TextUTF8					= 0x0A;//001010
	public final static int TextUTF8Optional			= 0x0B;//001011
	
	public final static int DecimalSingle				= 0x0C;//001100
	public final static int DecimalSingleOptional  	= 0x0D;//001101
	public final static int DecimalTwin	        	= 0x0E;//001110
	public final static int DecimalTwinOptional		= 0x0F;//001111
	
	public final static int ByteArray	        		= 0x10;//010000
	public final static int ByteArrayOptional			= 0x11;//010001	
	
	
	//lots of room for the next revision, eg booleans and enums

}
