package com.ociweb.pronghorn.pipe.token;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

public class LOCUtil {

    public static boolean isLocOfAnyType(int fieldLoc, int type1) {        
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1;
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2) {
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2;
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2, int type3) {
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2 ||
               extractedType==type3;
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2, int type3, int type4) {
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2 ||
               extractedType==type3 ||
               extractedType==type4;
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2, int type3, int type4, int type5) {
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2 ||
               extractedType==type3 ||
               extractedType==type4 ||
               extractedType==type5; 
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2, int type3, int type4, int type5, int type6) {
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2 ||
               extractedType==type3 ||
               extractedType==type4 ||
               extractedType==type5 ||
               extractedType==type6;
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2, int type3, int type4, int type5, int type6, int type7) {
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2 ||
               extractedType==type3 ||
               extractedType==type4 ||
               extractedType==type5 ||
               extractedType==type6 ||
               extractedType==type7;
    }

    public static boolean isLocOfAnyType(int fieldLoc, int type1, int type2, int type3, int type4, int type5, int type6, int type7, int type8) {        
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(fieldLoc);
        return extractedType==type1 || 
               extractedType==type2 ||
               extractedType==type3 ||
               extractedType==type4 ||
               extractedType==type5 ||
               extractedType==type6 ||
               extractedType==type7 ||
               extractedType==type8;
    }

    public static String typeAsString(int loc) {
        assert(0==(loc>>31)) : "This is not a LOC";
        int extractedType = FieldReferenceOffsetManager.extractTypeFromLoc(loc);
        System.out.println("extracted type "+extractedType+"  "+Integer.toBinaryString(extractedType)+" extracted from "+Integer.toBinaryString(loc));
        
        FieldReferenceOffsetManager.extractTypeFromLoc(loc);
        
        return TypeMask.methodTypeName[extractedType] + TypeMask.methodTypeSuffix[extractedType];
        
    }

}
