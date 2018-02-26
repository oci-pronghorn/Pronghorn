package com.ociweb.pronghorn.struct;

public enum BStructTypes {
	////////////////////////////////////////////////////
	//do not modify this order since the ordinal values may be saved
	////////////////////////////////////////////////////
	Boolean, //JSON 
	Text,    //JSON URL     UTF8 encoded with packed int length
	Decimal, //JSON URL
	Long,    //JSON URL     Packed
	Integer, //             Packed
	Short,
	Byte,
	Rational,//     URL
	Double,
	Float,
	
}
