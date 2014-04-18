//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

public class TokenBuilder {

    /*
     * 32 bits total****************** 1 token flag 5 type (1 new 2x spec types,
     * 3 existing types, 1 isOptional) 6 operation (for decimal its 3 and 3) all
     * others use bottom 4 2 value of null 18 instance - max value 262144 (field
     * id OR script steps inside this group)
     * 
     * read the type first then each can have its own operation parse logic.
     * this is used by decimal in order to get two operations groups and
     * dictionarys have their own operators as well
     * 
     * 
     * groups often use multiple tokens in a row from the script to satify the
     * required arguments. group type operators - Open/Close, Repeats(second
     * int) read type operators - Read from (into next field in use)
     */

    public static final int MAX_INSTANCE = 0x3FFFF; // 2^18 max fields 262144
    // See fast writer for details and mask sizes
    public static final int MASK_TYPE = 0x1F; // 5 bits

    public static final int MAX_FIELD_ID_BITS = 18;
    public static final int MAX_FIELD_ID_VALUE = (1 << MAX_FIELD_ID_BITS) - 1;
    public static final int MAX_FIELD_MASK = 0xFFFFFFFF ^ MAX_FIELD_ID_VALUE;

    public static final int SHIFT_ABSENT = MAX_FIELD_ID_BITS;
    public static final int BITS_ABSENT = 2;
    public static final int SHIFT_OPER = SHIFT_ABSENT + BITS_ABSENT;
    public static final int BITS_OPER = 6;
    public static final int SHIFT_TYPE = SHIFT_OPER + BITS_OPER;
    public static final int BITS_TYPE = 5;

    public static final int MASK_ABSENT = 0x3; // 2 bits
    public static final int MASK_ABSENT_DEFAULT = 0x3; // 2 bits //default value

    public static final int MASK_OPER = 0x3F; // 6 bits
    public static final int MASK_OPER_DECIMAL_EX = 0x07; // 3 bits
    public static final int SHIFT_OPER_DECIMAL_EX = 3;

    // sequence is stored as a length field type which appears in the stream
    // before the repeating children.
    // an optional sequence will use the optional length as its encoding to be
    // absent.
    // template ref must appear before length because it can modify length
    // operator.

    public static int extractType(int token) {
        return (token >>> TokenBuilder.SHIFT_TYPE) & TokenBuilder.MASK_TYPE;
    }

    public static int extractOper(int token) {
        return (token >>> TokenBuilder.SHIFT_OPER) & TokenBuilder.MASK_OPER;
    }

    public static int extractAbsent(int token) {
        return (token >>> TokenBuilder.SHIFT_ABSENT) & TokenBuilder.MASK_ABSENT;
    }

    public static boolean isOptional(int token) {
        return (0 != (token & (1 << TokenBuilder.SHIFT_TYPE)));
    }

    public static CharSequence methodNameRead(int token, StringBuilder target) {
        int type = extractType(token);

        String typeName = TypeMask.methodTypeInstanceName[type];

        int oper = extractOper(token);
        target.append("outputQueue.append").append(typeName).append("(");

        if (!typeName.isEmpty()) {
            target.append("reader").append(typeName).append('.');
        }

        target.append("read").append(TypeMask.methodTypeName[type]).append(OperatorMask.methodOperatorName[oper])
                .append(TypeMask.methodTypeSuffix[type]).append("(token,readFromIdx)").append(");");
        return target;
    }

    // Decimals must pass in both operators in the tokenOpps field together
    public static int buildToken(int tokenType, int tokenOpps, int count, int absentVal) {
        assert (count <= MAX_INSTANCE);
        assert (TypeMask.toString(tokenType).indexOf("unknown") == -1) : "Unknown type of " + tokenType + " "
                + Integer.toHexString(tokenType);
        assert (tokenType >= 0);
        assert (tokenType <= MASK_TYPE);
        assert (tokenOpps >= 0);
        assert (tokenOpps <= MASK_OPER) : "Opps " + Integer.toHexString(tokenOpps);

        return 0x80000000 | (tokenType << TokenBuilder.SHIFT_TYPE) | (tokenOpps << TokenBuilder.SHIFT_OPER)
                | (absentVal << TokenBuilder.SHIFT_ABSENT) | count & MAX_INSTANCE;

    }

    public static boolean isInValidCombo(int type, int operator) {
        if (type >= 0 && type <= TypeMask.LongSignedOptional) {
            // integer/long types do not support tail
            if (OperatorMask.Field_Tail == operator) {
                return true;
            }
        }

        return false;
    }

    public static boolean isOpperator(int token, int operator) {
        int type = extractType(token);

        if (type == TypeMask.Decimal || type == TypeMask.DecimalOptional) {
            return ((token >> TokenBuilder.SHIFT_OPER) & TokenBuilder.MASK_OPER_DECIMAL_EX) == operator
                    || ((token >> (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)) & TokenBuilder.MASK_OPER_DECIMAL_EX) == operator;
        } else {
            return ((token >> TokenBuilder.SHIFT_OPER) & TokenBuilder.MASK_OPER) == operator;
        }
    }

    public static String tokenToString(int token) {
        if (token == -1) {
            return "Unknown";
        }
        int type = extractType(token);
        int count = token & TokenBuilder.MAX_INSTANCE;

        if (type == TypeMask.Decimal || type == TypeMask.DecimalOptional) {
            int opp1 = (token >> (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX))
                    & TokenBuilder.MASK_OPER_DECIMAL_EX;
            int opp2 = (token >> TokenBuilder.SHIFT_OPER) & TokenBuilder.MASK_OPER_DECIMAL_EX;
            if (isInValidCombo(type, opp1)) {
                throw new UnsupportedOperationException("bad token");
            }
            ;
            if (isInValidCombo(type, opp2)) {
                throw new UnsupportedOperationException("bad token");
            }
            ;
            return (TypeMask.toString(type) + "/" + OperatorMask.toString(type, opp1) + "/"
                    + OperatorMask.toString(type, opp2) + "/" + count);

        } else {
            int opp = (token >> TokenBuilder.SHIFT_OPER) & TokenBuilder.MASK_OPER;
            if (isInValidCombo(type, opp)) {
                throw new UnsupportedOperationException("bad token");
            }
            ;

            return (TypeMask.toString(type) + "/" + OperatorMask.toString(type, opp) + "/" + count);
        }

    }

    /**
     * Computes the absent values as needed. 00 -> 1 01 -> 0 10 -> -1 11 ->
     * TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT
     * 
     * 0 1 11111111111111111111111111111111 1111111111111111111111111111111
     * 
     * @param b
     * @return
     */
    public static final int absentValue32(int b) {
        return ((1 | (0 - (b >> 1))) >>> (1 & b));
    }

    public static boolean isPowerOfTwo(int length) {

        while (0 == (length & 1)) {
            length = length >> 1;
        }
        return length == 1;
    }

    /**
     * Computes the absent values as needed. 00 -> 1 01 -> 0 10 -> -1 11 ->
     * TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG
     * 
     * 0 1 1111111111111111111111111111111111111111111111111111111111111111
     * 111111111111111111111111111111111111111111111111111111111111111
     * 
     * @param b
     * @return
     */
    public static long absentValue64(int b) {
        return ((1 | (0l - (b >> 1))) >>> (1 & b));
    }

}
