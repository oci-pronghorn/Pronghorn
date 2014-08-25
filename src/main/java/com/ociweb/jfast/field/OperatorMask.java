//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

public class OperatorMask {

    // 4 bits required for operator

    public static final int Field_None = 0x00; // 0000 //group
    public static final int Field_Copy = 0x01; // 0001 //field copy open/close
                                               // templated repeat
    public static final int Field_Constant = 0x02; // 0010 //open
    public static final int Field_Default = 0x03; // 0011 //open
    public static final int Field_Delta = 0x04; // 0100 //close
    public static final int Field_Increment = 0x05; // 0101 //close

    public static final int Field_Tail = 0x08; // 1000 // NEVER NUMERIC

    public static final int Dictionary_Reset = 0x00; // 0000
    public static final int Dictionary_Read_From = 0x01; // 0001 //prefix for
                                                         // next normal field,
                                                         // read from this into
                                                         // that.
    // NOTE: ReadFrom is implemented as copy today but will be redone in the
    // future as a real read from.

    // This is not the full mask like the others but instead bits that may be
    // used in any combination.
    public static final int Group_Bit_Close = 0x01; // count value will be
                                                    // tokens back to top,
                                                    // otherwise pmap max bytes.
    public static final int Group_Bit_Templ = 0x02; // template must be found
                                                    // before this group
    public static final int Group_Bit_Seq = 0x04; // use length field and use
                                                  // jump back logic
    public static final int Group_Bit_PMap = 0x08; // group requires a pmap
    public static final int Group_Bit_Msg = 0x10;//TODO: should be used in building new tokens to help processing messages.

    // group, sequence, message or ...??
    // pmap is only in group or sequence never message
    //

    public final static String[] xmlOperatorName = new String[] { "", "copy", "constant", "default", "delta",
        "increment", "Reserved6", "Reserved7", "tail", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "" };
    
    public final static String[] methodOperatorName = new String[] { "", "Copy", "Constant", "Default", "Delta",
            "Increment", "Reserved6", "Reserved7", "Tail", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
            "", "", "", "", "", "" };

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
    public static String toString(int type, int opp) {

        switch (type) {
        case TypeMask.Group:
            StringBuilder builder = new StringBuilder();
            if (0 != (Group_Bit_Close & opp)) {
                builder.append("Close:");
            } else {
                builder.append("Open:");
            }

            if (0 != (Group_Bit_Templ & opp)) {
                builder.append("DynTempl:");
            }

            if (0 != (Group_Bit_Seq & opp)) {
                builder.append("Seq:");
            }

            if (0 != (Group_Bit_PMap & opp)) {
                builder.append("PMap:");
            }

            return builder + ":" + prefix(6, '0', Integer.toBinaryString(opp));

        case TypeMask.Dictionary:
            switch (opp) {
            case Dictionary_Reset:
                return "Reset:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Dictionary_Read_From:
                return "ReadFrom:" + prefix(6, '0', Integer.toBinaryString(opp));
            default:
                return "unknown operation:" + prefix(6, '0', Integer.toBinaryString(opp));
            }
        default:
            switch (opp) {
            case Field_None:
                return "None:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Field_Copy:
                return "Copy:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Field_Constant:
                return "Constant:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Field_Default:
                return "Default:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Field_Delta:
                return "Delta:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Field_Increment:
                return "Increment:" + prefix(6, '0', Integer.toBinaryString(opp));
            case Field_Tail:
                return "Tail:" + prefix(6, '0', Integer.toBinaryString(opp));
            default:
                return "unknown operation:" + prefix(6, '0', Integer.toBinaryString(opp));
            }
        }
    }

}
