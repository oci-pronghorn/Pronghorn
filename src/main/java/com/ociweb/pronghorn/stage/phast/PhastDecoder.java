package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;

public class PhastDecoder {
	
	public static final int INCOMING_VARIABLE = -63;

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
    public static String decodeString(DataInputBlobReader writer) throws IOException{
    	if (DataInputBlobReader.readPackedInt(writer) == INCOMING_VARIABLE){
    		return writer.readUTF();
    	}
    	else 
    		return null;
    }
    //longs
    //decodes an increment long
    public static long decodeIncrementLong(long[] longDictionary, long map, int idx, int bitMask){
    	return (0==(map&bitMask))? ++longDictionary[idx] : longDictionary[idx];
    }
    
    //decodes present long
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
    
    //shorts
    //decodes an increment short
    public static short decodeIncrementShort(short[] shortDictionary, long map, int idx, int bitMask){
    	return (0==(map&bitMask))? ++shortDictionary[idx] : shortDictionary[idx];
    }
    
    //decodes present short
    public static short decodePresentShort(DataInputBlobReader reader, long map, int bitMask){
    	return(0==(map&bitMask))? reader.readPackedShort() : null;
    }
    //decode default short
    public static short decodeDefaultShort(DataInputBlobReader reader, long map, short[] defaultValues, int bitMask, int idx) {
        return (0==(map&bitMask)) ? defaultValues[idx] : reader.readPackedShort();
     }
    //decode copy short
    public static short decodeCopyShort(short[] shortDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        //always favor the more common zero case
        return (0==(map&bitMask)) ? shortDictionary[idx] : (shortDictionary[idx] = reader.readPackedShort());
    }
    
    public static short decodeDeltaShort(short[] shortDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        return (0==(map&bitMask)) ? (shortDictionary[idx] += reader.readPackedShort()) : shortDictionary[idx];        
    }
}
