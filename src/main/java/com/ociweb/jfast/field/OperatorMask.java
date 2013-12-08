package com.ociweb.jfast.field;

public class OperatorMask {

	//Offsets much match mask values of OperatorMask
	public static final String[] OPP_NAME = new String[]{"","copy","constant","default","delta","increment","tail"};
	
	//4 bits required for operator
	
	public static final int None      = 0x00;  //0000
	public static final int Copy 	     = 0x01;  //0001 
	public static final int Constant  = 0x02;  //0010
	public static final int Default   = 0x03;  //0011 
	public static final int Delta     = 0x04;  //0100
	public static final int Increment = 0x05;  //0101
	//RESSERVED 2 spots for future int/decimal operator
	//Because decimals only have 3 bits for operator of each part.
	public static final int Tail      = 0x08;  //1000 // NEVER NUMERIC
	
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
	public static String toString(int opp) {
		switch(opp) {
			case None:
				return "None:"+prefix(6,'0',Integer.toBinaryString(opp));
			case Copy:
				return "Copy:"+prefix(6,'0',Integer.toBinaryString(opp));
			case Constant:
				return "Constant:"+prefix(6,'0',Integer.toBinaryString(opp));
			case Default:
				return "Default:"+prefix(6,'0',Integer.toBinaryString(opp));
			case Delta:
				return "Delta:"+prefix(6,'0',Integer.toBinaryString(opp));
			case Increment:
				return "Increment:"+prefix(6,'0',Integer.toBinaryString(opp));
			case Tail:
				return "Tail:"+prefix(6,'0',Integer.toBinaryString(opp));
			default:	
				return "unknown operation:"+prefix(6,'0',Integer.toBinaryString(opp));
		}
	}
	
}
