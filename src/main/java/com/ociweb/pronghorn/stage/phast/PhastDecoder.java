package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;

public class PhastDecoder {

    public static long decodeDeltaLong(long[] longDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        return (0==(map&bitMask)) ? (longDictionary[idx] += DataInputBlobReader.readPackedLong(reader)) : longDictionary[idx];        
    }

    public static int decodeDefaultInt(DataInputBlobReader reader, long map, int[] defaultValues, int bitMask, int idx) {
       return (0==(map&bitMask)) ? defaultValues[idx] : DataInputBlobReader.readPackedInt(reader);
    }

    public static int decodeDeltaInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask, int value) {
        return (0==(map&bitMask)) ? (intDictionary[idx] += DataInputBlobReader.readPackedInt(reader)) : intDictionary[idx];
    }

    public static int decodeIncInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        //always favor the more common zero case
        return (0==(map&bitMask)) ? intDictionary[idx]++ : decodeIncIntSlow(intDictionary, reader, idx);
    }

    private static int decodeIncIntSlow(int[] intDictionary, DataInputBlobReader reader, int idx) {
        intDictionary[idx] = DataInputBlobReader.readPackedInt(reader);
        return intDictionary[idx]++;
    }

    public static int decodeCopyInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        //always favor the more common zero case
        return (0==(map&bitMask)) ? intDictionary[idx] : (intDictionary[idx] = DataInputBlobReader.readPackedInt(reader));
    }
    
    //decodes an increment int
    public static int decodeIncrementInt(int[] intDictionary, long map, int idx, int bitMask){
    	return (0==(map&bitMask))? ++intDictionary[idx] : intDictionary[idx];
    }
    
    //decodes present int
    public static int decodePresentInt(DataInputBlobReader reader, long map, int bitMask){
    	return(0==(map&bitMask))? DataInputBlobReader.readPackedInt(reader) : null;
    }
    
    //decodes string
    public static String decodeString(DataInputBlobReader slab, DataInputBlobReader blob) throws IOException{
    	if (DataInputBlobReader.readPackedInt(slab) == -63){
    		StringBuilder s = new StringBuilder();
    		DataInputBlobReader.readPackedChars(blob, s);
    		return s.toString();
    	}
    	else 
    		return null;
    }
    //longs
    //decodes an increment long
    public static long decodeIncrementLong(long[] longDictionary, long map, int idx, int bitMask){
    	return (0==(map&bitMask))? ++longDictionary[idx] : longDictionary[idx];
    }
    
    //decodes present int
    public static long decodePresentLong(DataInputBlobReader reader, long map, int bitMask){
    	return(0==(map&bitMask))? DataInputBlobReader.readPackedLong(reader) : null;
    }
    //decode default long
    public static long decodeDefaultLong(DataInputBlobReader reader, long map, long[] defaultValues, int bitMask, int idx) {
        return (0==(map&bitMask)) ? defaultValues[idx] : DataInputBlobReader.readPackedLong(reader);
     }
    //decode copy long
    public static long decodeCopyLong(long[] longDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        //always favor the more common zero case
        return (0==(map&bitMask)) ? longDictionary[idx] : (longDictionary[idx] = DataInputBlobReader.readPackedLong(reader));
    }
}
