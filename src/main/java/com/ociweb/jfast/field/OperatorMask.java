package com.ociweb.jfast.field;

public class OperatorMask {

	//4 bits required for operator
	
	public final static int None      = 0x00;  //000
	public final static int Copy 	     = 0x01;  //001 
	public final static int Constant  = 0x02;  //010
	public final static int Default   = 0x03;  //011 
	public final static int Delta     = 0x04;  //100
	public final static int Increment = 0x05;  //101
	public final static int Tail      = 0x06;  //110
	
	
}
