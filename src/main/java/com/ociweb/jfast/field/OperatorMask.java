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
	
	public static final int Dictionary_Reset         = 0x00;  //0000
	public static final int Dictionary_Read_From     = 0x01;  //0001 //prefix for next normal field, read from this into that.
    //NOTE: ReadFrom is implemented as copy today but will be redone in the future as a real read from.
	
	
	//This is not the full mask like the others but instead bits that may be used in any combination.
	public static final int Group_Bit_Close          = 0x01; //count value will be tokens back to top, otherwise pmap max bytes.
	public static final int Group_Bit_Templ          = 0x02; //template must be found before this group
	public static final int Group_Bit_Seq            = 0x04; //use length field and use jump back logic
	public static final int Group_Bit_Msg            = 0x08; //this group is a full message
	public static final int Group_Bit_PMap           = 0x10; //group requires a pmap
	
	//group, sequence, message or ...??		
	//pmap is only in group or sequence never message
	//
	
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
				return "G:"+prefix(6,'0',Integer.toBinaryString(opp));
			case TypeMask.Dictionary:
				return "D:"+prefix(6,'0',Integer.toBinaryString(opp));
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
