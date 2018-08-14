package com.ociweb.pronghorn.util.parse;

public class JSONConstants {

	
	// Documentation
	// http://rfc7159.net/rfc7159
	//////////////////

	static final byte[] beginArray = new byte[]{0x5B}; //      [
	static final byte[] beginObject = new byte[]{0x7B}; //     {
	static final byte[] endArray = new byte[]{0x5D}; //        ]
	static final byte[] endObject = new byte[]{0x7D}; //       }
	static final byte[] nameSeparator = new byte[]{0x3A}; //   :
	static final byte[] valueSeparator = new byte[]{0x2C}; //  ,

	//WARNING: this does not match spec exactly no support TODO: yet for scientific notiation
	static final byte[] number = "%i%.".getBytes();
	
	static final byte[] falseLiteral = new byte[]{0x66,0x61,0x6c,0x73,0x65}; //false
	
	static final byte[] nullLiteral = new byte[]{0x6e,0x75,0x6c,0x6c}; //null
	
	static final byte[] trueLiteral = new byte[]{0x74,0x72,0x75,0x65}; //true
	
	static final byte[] ws1 = new byte[]{0x20}; //Space
	static final byte[] ws2 = new byte[]{0x09}; //Horizontal tab
	static final byte[] ws3 = new byte[]{0x0A}; //Line feed or New line
	static final byte[] ws4 = new byte[]{0x0D}; //Carriage return
	
	static final byte[] continuedString = new byte[]{0x5C};	

	static final byte[] string221 = new byte[]{0x22,'%','b',0x5C}; // "
	static final byte[] string222 = new byte[]{0x22,'%','b',0x22}; // " 
		
	static final byte[] string5C1 = new byte[]{0x5C,'%','b',0x5C}; // \
	static final byte[] string5C2 = new byte[]{0x5C,'%','b',0x22}; // \ 
			
	static final byte[] string2F1 = new byte[]{0x2F,'%','b',0x5C}; // /
	static final byte[] string2F2 = new byte[]{0x2F,'%','b',0x22}; // /
	
	static final byte[] string621 = new byte[]{0x62,'%','b',0x5C}; // backspace    
	static final byte[] string622 = new byte[]{0x62,'%','b',0x22}; // backspace
	
	static final byte[] string661 = new byte[]{0x66,'%','b',0x5C}; // form feed
	static final byte[] string662 = new byte[]{0x66,'%','b',0x22}; // form feed 

	static final byte[] string6E1 = new byte[]{0x6E,'%','b',0x5C}; // line feed        
	static final byte[] string6E2 = new byte[]{0x6E,'%','b',0x22}; // line feed 

	static final byte[] string721 = new byte[]{0x72,'%','b',0x5C}; // carriage return
	static final byte[] string722 = new byte[]{0x72,'%','b',0x22}; // carriage return 
	
	static final byte[] string741 = new byte[]{0x74,'%','b',0x5C}; // tab             
	static final byte[] string742 = new byte[]{0x74,'%','b',0x22}; // tab 
	
	static final byte[] string751 = new byte[]{0x75,'%','b',0x5C}; // uXXXX 4HexDig
	static final byte[] string752 = new byte[]{0x75,'%','b',0x22}; // uXXXX 4HexDig	 
	
	
}
