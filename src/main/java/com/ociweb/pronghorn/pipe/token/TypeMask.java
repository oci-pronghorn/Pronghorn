//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.pronghorn.pipe.token;

public final class TypeMask {

    // each group of "similar" types must stay together as a block.

	//                                                                    alternate numeric (optional bits mask 1???)
	//                                                                    takes up same space and can have same compression just typed differently
    // 1
    public final static int IntegerUnsigned = 0x00;        // 00000 even  small/boolean  actual bit count defined in from.
    public final static int IntegerUnsignedOptional = 0x01;// 00001 odd   small/booleanOptional
    public final static int IntegerSigned = 0x02;          // 00010 even  float32
    public final static int IntegerSignedOptional = 0x03;  // 00011 odd   float32Optional

    // 2
    public final static int LongUnsigned = 0x04;           // 00100
    public final static int LongUnsignedOptional = 0x05;   // 00101
    public final static int LongSigned = 0x06;             // 00110       double64
    public final static int LongSignedOptional = 0x07;     // 00111       double64Optional
    
    // 2
    public final static int TextASCII = 0x08;              // 01000
    public final static int TextASCIIOptional = 0x09;      // 01001
    public final static int TextUTF8 = 0x0A;               // 01010
    public final static int TextUTF8Optional = 0x0B;       // 01011

    // 3
    public final static int Decimal = 0x0C;                // 01100       real (64/32)
    public final static int DecimalOptional = 0x0D;        // 01101       realOptional
    // 2
    public final static int ByteVector = 0x0E;             // 01110    (position and length) each in 32 bit int, high bits of position hold const and runtime rune/bcd encoding).
    public final static int ByteVectorOptional = 0x0F;     // 01111
    
    public final static int Group = 0x10;                  // 10000 
 // public final static int OptionalGroup = 0x11;          // 10001 //not yet supported 

    
    //for sequence this is an uint32
    public final static int GroupLength = 0x14;            // 10100   10?00
    
    													   // 10110 //Add new data type here
                                                           // 10111 //Add new data type here
    
    public final static int Dictionary = 0x18;             // 11000
    
    
    public final static int Rational = 0x1A;               // 11010
    public final static int RationalOptional = 0x1B;       // 11011 

    //                                                        11110 //TODO: reserved for 128 ints?
    //                                                        11111 //TODO: reserved for 128 ints?
        
    public final static int[] ringBufferFieldSize = new int[] {   1, 1, 1, 1, //integers
                                                                  2, 2, 2, 2, //longs 
                                                                  2, 2, 2, 2, //text
                                                                  1, 1, 2, 2, //Decimals are represented as 2 tokens in the script this size only need show the first  
                                                                  0, 0, 0, 0,
                                                                  1, 0, 0, 0, //lenght
                                                                  0, 0, 4, 4};//dictionary X Rationals

    public final static int[] ringBufferFieldVarLen = new int[] {   0, 0, 0, 0, 
															        0, 0, 0, 0, 
															        1, 1, 1, 1, 
															        0, 0, 1, 1,  
															        0, 0, 0, 0,
															        0, 0, 0, 0,
															        0, 0, 0, 0};

    //NOTE: if type is Decimal, jump by 2
    public final static int[] scriptTokenSize = new int[] {   1, 1, 1, 1, 
                                                              1, 1, 1, 1, 
                                                              1, 1, 1, 1, 
                                                              2, 2, 1, 1,  
                                                              1, 1, 1, 1,
                                                              1, 1, 1, 1,
                                                              1, 1, 1, 1};
    
    public final static String[] xmlTypeName = new String[] { "uInt32", "uInt32", "int32", "int32", "uInt64", "uInt64", "int64", "int64",
        "string", "string", "string", "string", "decimal", "decimal", "byteVector", "byteVector",
        "group", "Reserved1", "Reserved2", "Reserved3", "length", "Reserved5",
        "Reserved6", "Reserved7", "Dictionary", "Reserved8", "rational", "rational" };


    // for code generation need to know the substring of the method related to
    // this type.
    public final static String[] methodTypeName = new String[] { "IntegerUnsigned", "IntegerUnsigned", "IntegerSigned", "IntegerSigned", 
    		                                                     "LongUnsigned", "LongUnsigned", "LongSigned", "LongSigned",
    		                                                     "ASCII", "ASCII", "UTF8", "UTF8",
                                                                 "Decimal", "Decimal", "Bytes", "Bytes", 
                                                                 "Group", "Reserved1", "Reserved2", "Reserved3", 
                                                                 "Length", "Reserved5", "Reserved6", "Reserved7", 
                                                                 "Dictionary", "Reserved8", "Rational", "Rational" };

    public final static String[] methodTypeInstanceName = new String[] { "Integer", "Integer", "Integer", "Integer",
                                                                         "Long", "Long", "Long", "Long", 
                                                                         "Text", "Text", "Text", "Text",
															            "Decimal", "Decimal", "Bytes", "Bytes",            
															            "", "Reserved1", "Reserved2", "Reserved3", 
															            "Integer", "Reserved5", "Reserved6", "Reserved7",
															            "this","","Rational","Rational"};
    

    public final static String[] methodTypeSuffix = new String[] { "", "Optional", "", "Optional",
    		                                                       "", "Optional", "", "Optional", 
    		                                                       "", "Optional", "", "Optional", 
    		                                                       "", "Optional", "", "Optional", 
    		                                                       "", "", "", "", 
    		                                                       "", "", "", "",
    		                                                       "", "", "", "Optional"};
    
    
    public final static String[] primitiveTypes = new String[] { "int", "int", "int", "int",
    		                                                     "long", "long", "long", "long", 
    		                                                     "CharSequence", "CharSequence", "CharSequence", "CharSequence",
    		                                                     "Decimal", "Decimal", "DataInputBlobReader", "DataInputBlobReader",
    		                                                     "Group", "Reserved1", "Reserved2", "Reserved3", 
    		                                                     "Length", "Reserved5", "Reserved6", "Reserved7", 
    		                                                     "Dictionary", "", "Rational", "Rational"};

    private static String prefix(int len, char c, String value) {
        StringBuilder builder = new StringBuilder();
        int add = len - value.length();
        while (--add >= 0) {
            builder.append(c);
        }
        builder.append(value);
        return builder.toString();

    }
    
    public static boolean isDecimal(int type) {
        
        return type==TypeMask.Decimal ||
               type==TypeMask.DecimalOptional;
                
    }

    public static boolean isLong(int type) {
        
        return type==TypeMask.LongSigned ||
               type==TypeMask.LongSignedOptional ||
               type==TypeMask.LongUnsigned ||
               type==TypeMask.LongUnsignedOptional;
                
    }
    
    public static boolean isRational(int type) {
        
        return type==TypeMask.Rational ||
               type==TypeMask.RationalOptional;
                
    }
    
    public static boolean isInt(int type) {
        
        return type==TypeMask.GroupLength ||
               type==TypeMask.IntegerSigned ||
               type==TypeMask.IntegerSignedOptional ||
               type==TypeMask.IntegerUnsigned ||
               type==TypeMask.IntegerUnsignedOptional;
                
    }
    
    public static boolean isText(int type) {
        
        return type==TypeMask.TextASCII ||
               type==TypeMask.TextASCIIOptional ||
               type==TypeMask.TextUTF8 ||
               type==TypeMask.TextUTF8Optional;
                
    }
    
    public static boolean isByteVector(int type) {
        
        return type==TypeMask.ByteVector ||
               type==TypeMask.ByteVectorOptional;
                
    }
    
    public static boolean isUnsigned(int type) {
        
        return type==TypeMask.GroupLength ||
               type==TypeMask.LongUnsigned ||
               type==TypeMask.LongUnsignedOptional ||
               type==TypeMask.IntegerUnsigned ||
               type==TypeMask.IntegerUnsignedOptional;
                
    }
    
    public static boolean isOptional(int type) {
        
        return 0!=(1&type);
                
    }
    
    public static String toString(int ... typeMask) {
    	StringBuilder result = new StringBuilder();
    	for(int i = 0;i<typeMask.length;i++) {
    		result.append(toString(typeMask[i]));
    		result.append(',');
    	}
    	if (result.length()>0) {
    		result.setLength(result.length()-1);
    	}
    	return result.toString();
    }    
    // This method for debugging and therefore can produce garbage.
    public static String toString(int typeMask) {

        return methodTypeName[typeMask] + methodTypeSuffix[typeMask];
        //+ ":" + prefix(6, '0', Integer.toBinaryString(typeMask));

    }
    
    
    
    
    
    
    
}
