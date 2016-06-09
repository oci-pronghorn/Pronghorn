package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;

import java.io.UnsupportedEncodingException;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public class PhastEncoder {
	
	public static final int INCOMING_VARIABLE = -63;
	//in pmap 0 is for the most frequent and 1 is for the less frequent
	public static final long MOST_FREQUENT_CASE = 0;
	public static final long LEAST_FREQUENT_CASE = 1;
	
	
	//encodes pmap one number at a time
	//takes the pmap (0 if it has not been started yet) and the token for the number
	public static long pmapBuilderLong(long pmap, int token, long curValue, long prevValue, long initValue, boolean isNull){
		//gets the operation from the token
		//get the type from the token, to see if it is optional or not
		int type = TokenBuilder.extractType(token);
		if (TypeMask.isOptional(type)){
			pmap = (pmap << 1) + (isNull? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
		}
		//build pmap according to operator
		int oper = TokenBuilder.extractOper(token);
		switch (oper) {
        	case OperatorMask.Field_Copy:
        		pmap = (pmap << 1) + (prevValue==curValue? MOST_FREQUENT_CASE:LEAST_FREQUENT_CASE);
        		break;
        	case OperatorMask.Field_Constant:
        		//this intentionally left blank, does nothing if constant
        		break;
        	case OperatorMask.Field_Default:
        		pmap = (pmap << 1) + (curValue==initValue? MOST_FREQUENT_CASE:LEAST_FREQUENT_CASE);
        		break;
        	case OperatorMask.Field_Delta:
        		pmap = (pmap << 1) + (curValue==initValue? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
        		break;
        	case OperatorMask.Field_Increment:
        		pmap = (pmap << 1) + ((1 + prevValue) == curValue? MOST_FREQUENT_CASE:LEAST_FREQUENT_CASE);
        		break;
		}
		return pmap;
	}
	
	//pmap builder for all cases of int
	public static long pmapBuilderInt(long pmap, int token, int curValue, int prevValue, int initValue, boolean isNull){
		//gets the operation from the token
		//get the type from the token, to see if it is optional or not
		int type = TokenBuilder.extractType(token);
		if (TypeMask.isOptional(type)){
			pmap = (pmap << 1) + (isNull? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
		}
		//build pmap according to operator
		int oper = TokenBuilder.extractOper(token);
		switch (oper) {
        	case OperatorMask.Field_Copy:
        		pmap = (pmap << 1) + (prevValue==curValue? MOST_FREQUENT_CASE:LEAST_FREQUENT_CASE);
        		break;
        	case OperatorMask.Field_Constant:
        		//this intentionally left blank, does nothing if constant
        		break;
        	case OperatorMask.Field_Default:
        		pmap = (pmap << 1) + (curValue==initValue? MOST_FREQUENT_CASE:LEAST_FREQUENT_CASE);
        		break;
        	case OperatorMask.Field_Delta:
        		pmap = (pmap << 1) + (curValue==initValue? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
        		break;
        	case OperatorMask.Field_Increment:
        		pmap = (pmap << 1) + ((1 + prevValue) == curValue? MOST_FREQUENT_CASE:LEAST_FREQUENT_CASE);
        		break;
		}
		return pmap;
	}
	
	//pmap builder for string, the only case it can have is if it is optional or not
	public static long pmapBuilderString(long pmap, int token,  boolean isNull){
		int type = TokenBuilder.extractType(token);
		if (TypeMask.isOptional(type)){
			pmap = (pmap << 1) + (isNull? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
		}
		return pmap;
	}
	
	public static void encodeIntPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, int value) {
        if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
            DataOutputBlobWriter.writePackedInt(writer, value);
        }
    }

	public static void encodeDeltaInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, int value) {
        if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
            DataOutputBlobWriter.writePackedInt(writer, value-intDictionary[idx]);
            intDictionary[idx] = value;
        }
        else{
        	DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
        }
    }

    
	public static void encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, long value) {
        if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
            
            DataOutputBlobWriter.writePackedLong(writer, value-longDictionary[idx]);
            longDictionary[idx] = value;             
        }
    }
    
    //this method encodes a string
	public static void encodeString(DataOutputBlobWriter writer, String value, long pmapHeader, int bitMask) throws IOException{
		if(MOST_FREQUENT_CASE == (pmapHeader&bitMask)){
			//encode -63 so it knows it is variable length
			//make constant -63
			DataOutputBlobWriter.writePackedInt(writer, INCOMING_VARIABLE);
    		 
			//write string using utf
			writer.writeUTF(value);
		}
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
	public static void incrementInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
    		intDictionary[idx]++;
    	}
    }
    
    //this method just uses the previous value that was sent
	public static void copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, int value){
    	if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
    		DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedInt(writer, value);
    	}
    }
    
    //encodes the default value from the default value dictionary
	public static void encodeDefaultInt(int[] defaultIntDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, int value){
    	if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)){
    		DataOutputBlobWriter.writePackedInt(writer, defaultIntDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedInt(writer, value);
    	}
    }
    
    //encodes long that is present in the pmap
	public static void encodeLongPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, long value) {
        if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
        	DataOutputBlobWriter.writePackedLong(writer, value);
        }
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
	public static void incrementLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
    		longDictionary[idx]++;
    	}
    }
    
    //this method just uses the previous value that was sent
	public static void copyLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, long value){
    	if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
    		DataOutputBlobWriter.writePackedLong(writer, longDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedLong(writer, value);
    	}
    }
    
    //encodes default value for a long
	public static void encodeDefaultLong(long[] defaultLongDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, long value){
    	if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)){
    		DataOutputBlobWriter.writePackedLong(writer, defaultLongDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedLong(writer, value);
    	}
    }
    
    //encodes short that is present in the pmap
	public static void encodeShortPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, short value) {
        if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
        	DataOutputBlobWriter.writePackedShort(writer, value);
        }
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
	public static void incrementShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
    		shortDictionary[idx]++;
    	}
    }
    
    //this method just uses the previous value that was sent
	public static void copyShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, short value){
    	if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
    		DataOutputBlobWriter.writePackedShort(writer, shortDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedShort(writer, value);
    	}
    }
    
    //encodes default value for a short
	public static void encodeDefaultShort(short[] defaultShortDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, short value){
    	if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)){
    		DataOutputBlobWriter.writePackedShort(writer, defaultShortDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedShort(writer, value);
    	}
    }
    
    //encodes the change in value of a short
	public static void encodeDeltaShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, short value) {
        if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
            DataOutputBlobWriter.writePackedShort(writer, (short)(value-shortDictionary[idx]));
            shortDictionary[idx] = value;             
        }
    }
    
}
