//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

public class OperatorMask {

	//Offsets much match mask values of OperatorMask
	public static final String[] OPP_NAME = new String[]{"","copy","constant","default","delta","increment","tail"};
	
	//4 bits required for operator
	
	public static final int Field_None      = 0x00;  //0000      //group
	public static final int Field_Copy 	   = 0x01;  //0001      //field copy      open/close templated repeat
	public static final int Field_Constant  = 0x02;  //0010      //open 
	public static final int Field_Default   = 0x03;  //0011      //open
	public static final int Field_Delta     = 0x04;  //0100      //close
	public static final int Field_Increment = 0x05;  //0101      //close
	//RESSERVED 2 spots for future int/decimal operator
	//Because decimals only have 3 bits for operator of each part.
	public static final int Field_Tail      = 0x08;  //1000 // NEVER NUMERIC
	
	
	public static final int Group_Bit_Close          = 0x01;
	public static final int Group_Bit_Templ          = 0x02;
	public static final int Group_Bit_Seq            = 0x04;
			
	                                          //    bits            STC
	public static final int Group_Open 				= 0x00;  //0000
	public static final int Group_Close 				= 0x01;  //0001
	public static final int Group_Open_Templ 			= 0x02;  //0010
	public static final int Group_Close_Templ 		= 0x03;  //0011	
	public static final int Group_Open_Seq 			= 0x04;  //0100 //read length before repeating, length may be constant
	public static final int Group_Close_Seq 			= 0x05;  //0101 //check count and return to top if needed. (contains delta jump)
	public static final int Group_Open_Templ_Seq 		= 0x06;  //0110
	public static final int Group_Close_Templ_Seq 	= 0x07;  //0111	
	
	public static final int Dictionary_Reset         = 0x00;  //0000
	public static final int Dictionary_Copy          = 0x01;  //0001 //prefix for next normal field, copy this into that.
	
	
	
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
	public static String toString(int type, int opp) {
		
		switch (type) {
			case TypeMask.Group:
				return "Unknown  TODO  Group";
			case TypeMask.Dictionary:
				return "Unknown  TODO  Dictionary";
			default:
				switch(opp) {
					case Field_None:
						return "None:"+prefix(6,'0',Integer.toBinaryString(opp));
					case Field_Copy:
						return "Copy:"+prefix(6,'0',Integer.toBinaryString(opp));
					case Field_Constant:
						return "Constant:"+prefix(6,'0',Integer.toBinaryString(opp));
					case Field_Default:
						return "Default:"+prefix(6,'0',Integer.toBinaryString(opp));
					case Field_Delta:
						return "Delta:"+prefix(6,'0',Integer.toBinaryString(opp));
					case Field_Increment:
						return "Increment:"+prefix(6,'0',Integer.toBinaryString(opp));
					case Field_Tail:
						return "Tail:"+prefix(6,'0',Integer.toBinaryString(opp));
					default:	
						return "unknown operation:"+prefix(6,'0',Integer.toBinaryString(opp));
				}
		}
	}
	
}
