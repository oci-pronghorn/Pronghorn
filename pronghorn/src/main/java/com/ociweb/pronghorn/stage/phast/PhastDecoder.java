package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.ChannelReader;

public class PhastDecoder {
	
	public static final int INCOMING_VARIABLE = -63;
	//in pmap 0 is for the most frequent and 1 is for the less frequent
	public static final long MOST_FREQUENT_CASE = 0;
	public static final long LEAST_FREQUENT_CASE = 1;

    public static long decodeDeltaLong(long[] longDictionary, ChannelReader reader, long map, int idx, long bitMask, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)){
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map&bitMask)) ? (longDictionary[idx] += reader.readPackedLong()) : longDictionary[idx];
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? (longDictionary[idx] += reader.readPackedLong()) : longDictionary[idx];
        }
    }

    public static int decodeDefaultInt(ChannelReader reader, long map, int[] defaultValues, long bitMask, int idx, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? defaultValues[idx] : reader.readPackedInt();
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? defaultValues[idx] : reader.readPackedInt();
        }
    }

    public static int decodeDeltaInt(int[] intDictionary, ChannelReader reader, long map, int idx, long bitMask, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? (intDictionary[idx] += reader.readPackedInt()) : intDictionary[idx];
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? (intDictionary[idx] += reader.readPackedInt()) : intDictionary[idx];
        }
    }

    public static int decodeCopyInt(int[] intDictionary, ChannelReader reader, long map, int idx, long bitMask, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? intDictionary[idx] : (intDictionary[idx] = reader.readPackedInt());
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? intDictionary[idx] : (intDictionary[idx] = reader.readPackedInt());
        }
    }
    
    //decodes an increment int
    public static int decodeIncrementInt(int[] intDictionary, long map, int idx, long bitMask, Boolean isOptional){
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? ++intDictionary[idx] : intDictionary[idx];
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? ++intDictionary[idx] : intDictionary[idx];
        }
    }
    
    //decodes present int
    public static int decodePresentInt(ChannelReader reader, long map, long bitMask, Boolean isOptional){
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? reader.readPackedInt() : null;
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? reader.readPackedInt() : null;
        }
    }
    
    //decodes string
    public static String decodeString(ChannelReader reader, Boolean isOptional){
    	if (reader.readPackedInt() == INCOMING_VARIABLE){
    		return reader.readUTF();
    	}
    	else
    		return null;
    }
    //longs
    //decodes an increment long
    public static long decodeIncrementLong(long[] longDictionary, long map, int idx, long bitMask, Boolean isOptional){
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? ++longDictionary[idx] : longDictionary[idx];
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? ++longDictionary[idx] : longDictionary[idx];
        }
    }
    
    //decodes present long
    public static long decodePresentLong(ChannelReader reader, long map, long bitMask, Boolean isOptional){
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? reader.readPackedLong() : null;
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? reader.readPackedLong() : null;
        }
    }
    //decode default long
    public static long decodeDefaultLong(ChannelReader reader, long map, long[] defaultValues, long bitMask, int idx, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? defaultValues[idx] : reader.readPackedLong();
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? defaultValues[idx] : reader.readPackedLong();
        }
     }
    //decode copy long
    public static long decodeCopyLong(long[] longDictionary, ChannelReader reader, long map, int idx, long bitMask, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? longDictionary[idx] : (longDictionary[idx] = reader.readPackedLong());
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? longDictionary[idx] : (longDictionary[idx] = reader.readPackedLong());
        }
    }
    
    //shorts
    //decodes an increment short
    public static short decodeIncrementShort(short[] shortDictionary, long map, int idx, long bitMask, Boolean isOptional){
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? ++shortDictionary[idx] : shortDictionary[idx];
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? ++shortDictionary[idx] : shortDictionary[idx];
        }
    }
    
    //decodes present short
    public static short decodePresentShort(ChannelReader reader, long map, long bitMask, Boolean isOptional){
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? reader.readPackedShort() : null;
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? reader.readPackedShort() : null;
        }
    }
    //decode default short
    public static short decodeDefaultShort(ChannelReader reader, long map, short[] defaultValues, long bitMask, int idx, Boolean isOptional) {
            if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
                bitMask = bitMask << 1;
                return (MOST_FREQUENT_CASE == (map & bitMask)) ? defaultValues[idx] : reader.readPackedShort();
            } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? defaultValues[idx] : reader.readPackedShort();
        }
     }
    //decode copy short
    public static short decodeCopyShort(short[] shortDictionary, ChannelReader reader, long map, int idx, long bitMask, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? shortDictionary[idx] : (shortDictionary[idx] = reader.readPackedShort());
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? shortDictionary[idx] : (shortDictionary[idx] = reader.readPackedShort());
        }
    }
    
    public static short decodeDeltaShort(short[] shortDictionary, ChannelReader reader, long map, int idx, long bitMask, Boolean isOptional) {
        if (isOptional && MOST_FREQUENT_CASE == (map & bitMask)) {
            bitMask = bitMask << 1;
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? (shortDictionary[idx] += reader.readPackedShort()) : shortDictionary[idx];
        } else {
            return (MOST_FREQUENT_CASE == (map & bitMask)) ? (shortDictionary[idx] += reader.readPackedShort()) : shortDictionary[idx];
        }
    }
}
