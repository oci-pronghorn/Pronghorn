//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.pronghorn.ring.token;

public final class TypeMask {

    // need 6 bits total for type mask
    // each group of "similar" types must stay together as a block.

    // 1
    public final static int IntegerUnsigned = 0x00;        // 00000 even
    public final static int IntegerUnsignedOptional = 0x01;// 00001 odd
    public final static int IntegerSigned = 0x02;          // 00010 even
    public final static int IntegerSignedOptional = 0x03;  // 00011 odd

    // 2
    public final static int LongUnsigned = 0x04;           // 00100
    public final static int LongUnsignedOptional = 0x05;   // 00101
    public final static int LongSigned = 0x06;             // 00110
    public final static int LongSignedOptional = 0x07;     // 00111

    // 2
    public final static int TextASCII = 0x08;              // 01000
    public final static int TextASCIIOptional = 0x09;      // 01001
    public final static int TextUTF8 = 0x0A;               // 01010
    public final static int TextUTF8Optional = 0x0B;       // 01011

    // 3
    public final static int Decimal = 0x0C;                // 01100
    public final static int DecimalOptional = 0x0D;        // 01101
    // 2
    public final static int ByteArray = 0x0E;              // 01110
    public final static int ByteArrayOptional = 0x0F;      // 01111

    public final static int Group = 0x10;                  // 10000 //TOOD: May want to use 10001 for optional group.
    
    public final static int TemplateRef = 0x12;            // 10010
    // 1
    //for sequence this is an uint32
    public final static int GroupLength = 0x14;            // 10100   10?00

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
															        0, 0, 1, 1,  //Decimal only counts exponent part
															        0, 0, 0, 0,
															        0, 0, 0, 0, 0};

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

    // This method for debugging and therefore can produce garbage.
    public static String toString(int typeMask) {

        return methodTypeName[typeMask] + methodTypeSuffix[typeMask] + ":"
                + prefix(6, '0', Integer.toBinaryString(typeMask));

    }

}
