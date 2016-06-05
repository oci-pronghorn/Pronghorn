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
    public final static int ByteVector = 0x0E;              // 01110    (position and length) each in 32 bit int, high bits of position hold const and runtime rune/bcd encoding).
    public final static int ByteVectorOptional = 0x0F;      // 01111
    
    @Deprecated
    public final static int ByteArray = ByteVector;// 0x0E;              // 01110    (position and length) each in 32 bit int, high bits of position hold const and runtime rune/bcd encoding).
    @Deprecated
    public final static int ByteArrayOptional = ByteVectorOptional;// 0x0F;      // 01111

    public final static int Group = 0x10;                  // 10000 
 // public final static int OptionalGroup = 0x11;          // 10001 //not yet supported 
    
    public final static int GoSub = 0x12;                  // 10010 for implementing unions and other complex structures
    
    
    // 1
    //for sequence this is an uint32
    public final static int GroupLength = 0x14;            // 10100   10?00

    //                                                              //bitset ? can be byte array or can be int or long?  Should be on the primary ring as fixed steps!!
    
    
    //                                                        11010 
    //                                                        11011 
    //                                                        11110 
    //                                                        11111 
    

    public final static int Dictionary = 0x18;             // 11000
    public final static int SpacerGap = 0x1C;              // 11100  //TODO: C, add call to this and gen method.  must add new SpacerGap  into the script as needed, set in the ClientConfig to inject to the template.
        
    public final static int[] ringBufferFieldSize = new int[] {   1, 1, 1, 1, 
                                                                  2, 2, 2, 2, 
                                                                  2, 2, 2, 2, 
                                                                  1, 1, 2, 2, //Decimals are represented as 2 tokens in the script this size only need show the first  
                                                                  0, 0, 0, 0,
                                                                  1, 0, 0, 0, 0};

    public final static int[] ringBufferFieldVarLen = new int[] {   0, 0, 0, 0, 
															        0, 0, 0, 0, 
															        1, 1, 1, 1, 
															        0, 0, 1, 1,  
															        0, 0, 0, 0,
															        0, 0, 0, 0, 0};

    //NOTE: if type is Decimal, jump by 2
    public final static int[] scriptTokenSize = new int[] {   1, 1, 1, 1, 
                                                              1, 1, 1, 1, 
                                                              1, 1, 1, 1, 
                                                              2, 2, 1, 1,  
                                                              1, 1, 1, 1,
                                                              1, 1, 1, 1, 1};
    
    public final static String[] xmlTypeName = new String[] { "uInt32", "uInt32", "int32", "int32", "uInt64", "uInt64", "int64", "int64",
        "string", "string", "string", "string", "decimal", "decimal", "byteVector", "byteVector",
        "group", "Reserved1", "Reserved2", "Reserved3", "length", "Reserved5",
        "Reserved6", "Reserved7", "Dictionary" };


    // for code generation need to know the substring of the method related to
    // this type.
    public final static String[] methodTypeName = new String[] { "IntegerUnsigned", "IntegerUnsigned", "IntegerSigned",
            "IntegerSigned", "LongUnsigned", "LongUnsigned", "LongSigned", "LongSigned", "ASCII", "ASCII", "UTF8",
            "UTF8",
            "Decimal", // need exponent and mantissa strings.
            "Decimal", "Bytes", "Bytes", "Group", "Reserved1", "Reserved2", "Reserved3", "Length", "Reserved5",
            "Reserved6", "Reserved7", "Dictionary" };

    public final static String[] methodTypeInstanceName = new String[] { "Integer", "Integer", "Integer", "Integer",
                                                                         "Long", "Long", "Long", "Long", 
                                                                         "Text", "Text", "Text", "Text",
            "Decimal", // need exponent and mantissa strings.
            "Decimal", "Bytes", "Bytes", "", "Reserved1", "Reserved2", "Reserved3", "", "Reserved5", "Reserved6",
            "Reserved7", "this" };

    public final static String[] methodTypeSuffix = new String[] { "", "Optional", "", "Optional", "", "Optional", "",
            "Optional", "", "Optional", "", "Optional", "", "Optional", "", "Optional", "", "", "", "", "", "", "", "",
            "" };
    
    
    public final static String[] primitiveTypes = new String[] { "int", "int", "int", "int", "long", "long", "long", "long", "CharSequence", "CharSequence", "CharSequence",
            "CharSequence", "Decimal", // need exponent and mantissa strings.
            "Decimal", "DataInputBlobReader", "DataInputBlobReader", "Group", "Reserved1", "Reserved2", "Reserved3", "Length", "Reserved5",
            "Reserved6", "Reserved7", "Dictionary" };

    // lots of room for the next revision, eg booleans and enums

    // special flag used internally by FASTDynamic* to know when to return
    // control back to the caller.
    // public final static int Stop = 0x1F;//11111

    private static String prefix(int len, char c, String value) {
        StringBuilder builder = new StringBuilder();
        int add = len - value.length();
        while (--add >= 0) {
            builder.append(c);
        }
        builder.append(value);
        return builder.toString();

    }
    
    public static boolean isLong(int type) {
        
        return type==TypeMask.LongSigned ||
               type==TypeMask.LongSignedOptional ||
               type==TypeMask.LongUnsigned ||
               type==TypeMask.LongUnsignedOptional;
                
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
    
    
    // This method for debugging and therefore can produce garbage.
    public static String toString(int typeMask) {

        return methodTypeName[typeMask] + methodTypeSuffix[typeMask] + ":"
                + prefix(6, '0', Integer.toBinaryString(typeMask));

    }
    
    
    
    
    
    
    
}
