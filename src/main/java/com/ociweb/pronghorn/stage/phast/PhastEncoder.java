package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;

import java.io.UnsupportedEncodingException;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
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
	public static long pmapBuilderLong(long pmap, int type, int oper, long curValue, long prevValue, long initValue, boolean isNull){
		//build pmap according to operator
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
		//get the type from the token, to see if it is optional or not
		if (TypeMask.isOptional(type)){
			pmap = (pmap << 1) + (isNull? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
		}
		return pmap;
	}
	
	//pmap builder for all cases of int
	public static long pmapBuilderInt(long pmap, int type, int oper, int curValue, int prevValue, int initValue, boolean isNull){
		//build pmap according to operator
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
		//get the type from the token, to see if it is optional or not
		if (TypeMask.isOptional(type)){
			pmap = (pmap << 1) + (isNull? LEAST_FREQUENT_CASE:MOST_FREQUENT_CASE);
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
	
	public static void encodeIntPresent(DataOutputBlobWriter writer, long pmapHeader, long bitMask, int value, Boolean isOptional) {
        if (isOptional){
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
                    DataOutputBlobWriter.writePackedInt(writer, value);
                }
            }
        }
        else{
            if (MOST_FREQUENT_CASE == (pmapHeader&bitMask)) {
                DataOutputBlobWriter.writePackedInt(writer, value);
            }
        }
    }

	public static void encodeDeltaInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, int value, Boolean isOptional) {
		if (isOptional) {
			if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
				bitMask = bitMask << 1;
				if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
					DataOutputBlobWriter.writePackedInt(writer, value - intDictionary[idx]);
					intDictionary[idx] = value;
				} else {
					DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
				}
			}
		} else {
			if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
				DataOutputBlobWriter.writePackedInt(writer, value - intDictionary[idx]);
				intDictionary[idx] = value;
			} else {
				DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
			}
		}
	}
    
	public static void encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, long value, Boolean isOptional) {
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedLong(writer, value - longDictionary[idx]);
                    longDictionary[idx] = value;
                } else {
                    DataOutputBlobWriter.writePackedULong(writer, longDictionary[idx]);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedLong(writer, value - longDictionary[idx]);
                longDictionary[idx] = value;
            } else {
                DataOutputBlobWriter.writePackedULong(writer, longDictionary[idx]);
            }
        }
    }
    
    //this method encodes a string
	public static void encodeString(DataOutputBlobWriter writer, StringBuilder value, long pmapHeader, long bitMask, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                bitMask = bitMask << 1;
                //encode -63 so it knows it is variable length
                //make constant -63
                DataOutputBlobWriter.writePackedInt(writer, INCOMING_VARIABLE);

                //write string using utf
                writer.writeUTF(value.toString());
            }
        }
        else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                //encode -63 so it knows it is variable length
                //make constant -63
                DataOutputBlobWriter.writePackedInt(writer, INCOMING_VARIABLE);

                //write string using utf
                writer.writeUTF(value.toString());
            }
        }
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
	public static void incrementInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    intDictionary[idx]++;
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                intDictionary[idx]++;
            }
        }
    }
    
    //this method just uses the previous value that was sent
	public static void copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, int value, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
                } else {
                    DataOutputBlobWriter.writePackedInt(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
            } else {
                DataOutputBlobWriter.writePackedInt(writer, value);
            }
        }
    }
    
    //encodes the default value from the default value dictionary
	public static void encodeDefaultInt(int[] defaultIntDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitmask, int idx, int value, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)){
                bitmask = bitmask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)) {
                    DataOutputBlobWriter.writePackedInt(writer, defaultIntDictionary[idx]);
                } else {
                    DataOutputBlobWriter.writePackedInt(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)) {
                DataOutputBlobWriter.writePackedInt(writer, defaultIntDictionary[idx]);
            } else {
                DataOutputBlobWriter.writePackedInt(writer, value);
            }
        }
    }
    
    //encodes long that is present in the pmap
	public static void encodeLongPresent(DataOutputBlobWriter writer, long pmapHeader, long bitMask, long value, Boolean isOptional) {
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedLong(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedLong(writer, value);
            }
        }
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
	public static void incrementLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    longDictionary[idx]++;
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                longDictionary[idx]++;
            }
        }
    }
    
    //this method just uses the previous value that was sent
	public static void copyLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, long value, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedLong(writer, longDictionary[idx]);
                } else {
                    DataOutputBlobWriter.writePackedLong(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedLong(writer, longDictionary[idx]);
            } else {
                DataOutputBlobWriter.writePackedLong(writer, value);
            }
        }
    }
    
    //encodes default value for a long
	public static void encodeDefaultLong(long[] defaultLongDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitmask, int idx, long value, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)){
                bitmask = bitmask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)) {
                    DataOutputBlobWriter.writePackedLong(writer, defaultLongDictionary[idx]);
                } else {
                    DataOutputBlobWriter.writePackedLong(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)) {
                DataOutputBlobWriter.writePackedLong(writer, defaultLongDictionary[idx]);
            } else {
                DataOutputBlobWriter.writePackedLong(writer, value);
            }
        }
    }
    
    //encodes short that is present in the pmap
	public static void encodeShortPresent(DataOutputBlobWriter writer, long pmapHeader, long bitMask, short value, Boolean isOptional) {
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedShort(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedShort(writer, value);
            }
        }
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
	public static void incrementShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    shortDictionary[idx]++;
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                shortDictionary[idx]++;
            }
        }
    }
    
    //this method just uses the previous value that was sent
	public static void copyShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitMask, int idx, short value, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedShort(writer, shortDictionary[idx]);
                } else {
                    DataOutputBlobWriter.writePackedShort(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedShort(writer, shortDictionary[idx]);
            } else {
                DataOutputBlobWriter.writePackedShort(writer, value);
            }
        }
    }
    
    //encodes default value for a short
	public static void encodeDefaultShort(short[] defaultShortDictionary, DataOutputBlobWriter writer, long pmapHeader, long bitmask, int idx, short value, Boolean isOptional){
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)){
                bitmask = bitmask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)) {
                    DataOutputBlobWriter.writePackedShort(writer, defaultShortDictionary[idx]);
                } else {
                    DataOutputBlobWriter.writePackedShort(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitmask)) {
                DataOutputBlobWriter.writePackedShort(writer, defaultShortDictionary[idx]);
            } else {
                DataOutputBlobWriter.writePackedShort(writer, value);
            }
        }
    }
    
    //encodes the change in value of a short
	public static void encodeDeltaShort(short[] shortDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, long bitMask, short value, Boolean isOptional) {
        if (isOptional) {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)){
                bitMask = bitMask << 1;
                if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                    DataOutputBlobWriter.writePackedShort(writer, (short) (value - shortDictionary[idx]));
                    shortDictionary[idx] = value;
                } else{
                    DataOutputBlobWriter.writePackedShort(writer, value);
                }
            }
        } else {
            if (MOST_FREQUENT_CASE == (pmapHeader & bitMask)) {
                DataOutputBlobWriter.writePackedShort(writer, (short) (value - shortDictionary[idx]));
                shortDictionary[idx] = value;
            }else{
                DataOutputBlobWriter.writePackedShort(writer, value);
            }
        }
    }
    
}
