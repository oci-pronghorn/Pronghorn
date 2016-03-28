package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;

public class PhastDecoder {

    static long decodeDeltaLong(long[] longDictionary, DataInputBlobReader reader, long map, int idx, long defaultValue, int bitMask) {
        return (0==(map&bitMask)) ? (longDictionary[idx] += DataInputBlobReader.readPackedLong(reader)) : defaultValue;        
    }

    static int decodeDefaultInt(DataInputBlobReader reader, long map, int[] defaultValues, int bitMask, int idx) {
       return (0==(map&bitMask)) ? defaultValues[idx] : DataInputBlobReader.readPackedInt(reader);
    }

    static int decodeDeltaInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask, int value) {
        return (0==(map&bitMask)) ? (intDictionary[idx] += DataInputBlobReader.readPackedInt(reader)) : intDictionary[idx];
    }

    static int decodeIncInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        //always favor the more common zero case
        return (0==(map&bitMask)) ? intDictionary[idx]++ : decodeIncIntSlow(intDictionary, reader, idx);
    }

    private static int decodeIncIntSlow(int[] intDictionary, DataInputBlobReader reader, int idx) {
        intDictionary[idx] = DataInputBlobReader.readPackedInt(reader);
        return intDictionary[idx]++;
    }

    static int decodeCopyInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int bitMask) {
        //always favor the more common zero case
        return (0==(map&bitMask)) ? intDictionary[idx] : (intDictionary[idx] = DataInputBlobReader.readPackedInt(reader));
    }
    
    //decodes an increment int
    static int decodeIncrementInt(int[] intDictionary, long map, int idx, int bitMask){
    	return (0==(map&bitMask))? ++intDictionary[idx] : intDictionary[idx];
    }
    
    //decodes present int
    static int decodePresentInt(DataInputBlobReader reader, long map, int bitMask){
    	return(0==(map&bitMask))? DataInputBlobReader.readPackedInt(reader) : null;
    }
    
    //decodes string
    static String decodeString(DataInputBlobReader slab, DataInputBlobReader blob) throws IOException{
    	if (DataInputBlobReader.readPackedInt(slab) == -63){
    		StringBuilder s = new StringBuilder();
    		DataInputBlobReader.readPackedChars(blob, s);
    		return s.toString();
    	}
    	else{
    		return "FATALITY";
    	}
    	
    }
}
