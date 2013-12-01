package com.ociweb.jfast.field;

public class OperatorMask {

	//Offsets much match mask values of OperatorMask
	public static final String[] OPP_NAME = new String[]{"","copy","constant","default","delta","increment","tail"};
	
	//4 bits required for operator
	
	public static final int None      = 0x00;  //000
	public static final int Copy 	     = 0x01;  //001 
	public static final int Constant  = 0x02;  //010
	public static final int Default   = 0x03;  //011 
	public static final int Delta     = 0x04;  //100
	public static final int Increment = 0x05;  //101
	public static final int Tail      = 0x06;  //110
	
	
}
