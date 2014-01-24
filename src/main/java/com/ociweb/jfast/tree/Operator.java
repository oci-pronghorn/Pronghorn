package com.ociweb.jfast.tree;


public enum Operator {
	
	//TODO: Feature: add skip type to read the bytes not not consume them if client declares it not want the data.
	
	
	/* COMPACT NOTATION - TODO: need to build this notation from the template tree 
	 * 
	 * ((tag number)(data type)(field encoding operator))
	 *  | as dilimiter between fields
	 *  < start of repeating group
	 *  > end of repeating group
	 * 
	 * 
	 
	 
	! Default Coding – default value per template
	= Copy Coding – copy prior value
	+ Increment Coding – increment prior value
	- Delta Coding – numeric or string differential
	@ Constant Value Coding - constant value specified in template
	* Derived Value Coding – implies field values


	s String data type                 TODO: WHAT IS THE DIF BETWEEN ASCII AND UTF-8?
	u Unsigned integer data type
	U Unsigned integer data type supporting a NULL value
	i Signed integer data type
	I Signed integer data type supporting a NULL value
	F Scaled number data type                CLASSIC SCALED VALUE WITH SHARED OPERATOR TYPE
	n Composite scaled number                TWO FIELDS WITH SEPARATE OPERATORS
	v Byte vector data type


	EXAMPLE
	8s!FIX.4.4|9u|35s!X|49s=|34u+1|268u<279u=|269s=|55s=|167s=|270F-|271F-|346u-|276s=|277s=>



	 * 
	 */ 
	
	None ,
	Copy , 
	Constant ,
	Default ,
	Delta ,
	Tail ,
	Increment;

	
    
}
