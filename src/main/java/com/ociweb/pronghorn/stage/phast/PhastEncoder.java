package com.ociweb.pronghorn.stage.phast;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public class PhastEncoder {

    static void encodeIntPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, int value) {
        if (0 != (pmapHeader&bitMask)) {
            DataOutputBlobWriter.writePackedUInt(writer, value);
           // DataOutputBlobWriter.writePackedInt(writer, value); 
        }
    }

    static void encodeDeltaInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx, int value) {
        if (0 != (pmapHeader&bitMask)) {
            DataOutputBlobWriter.writePackedInt(writer, value-intDictionary[idx]);
            intDictionary[idx] = value;
        }
        else{
        	DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
        }
    }

    
    static void encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, long value) {
        if (0 != (pmapHeader&bitMask)) {
            
            DataOutputBlobWriter.writePackedLong(writer, value-longDictionary[idx]);
            longDictionary[idx] = value;             
        }
    }
    
    //this method encodes a string
    static void encodeString(DataOutputBlobWriter slab, DataOutputBlobWriter blob, String value) throws UnsupportedEncodingException{
    	//encode -63 so it knows it is variable length
    	DataOutputBlobWriter.writePackedInt(slab, -63);
    		 
    	//calculate string length in bytes, then encode it
    	byte[] byteArray = value.getBytes("UTF-16BE");
    	DataOutputBlobWriter.writePackedInt(slab, byteArray.length);
    	DataOutputBlobWriter.writePackedChars(blob, value);
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
    static void incrementInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (0 != (pmapHeader&bitMask)) {
    		intDictionary[idx]++;
    	}
    		DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
    }
    
    //this method just uses the previous value that was sent
    static void copyInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (0 == (pmapHeader&bitMask)) {
    		DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
    	}
    }
    
    //encodes the default value from the default value dictionary
    static void encodeDefaultInt(int[] defaultIntDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, int value){
    	if (0 == (pmapHeader & bitmask)){
    		DataOutputBlobWriter.writePackedInt(writer, defaultIntDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedInt(writer, value);
    	}
    }
    
    //encodes long that is present in the pmap
    static void encodeLongPresent(DataOutputBlobWriter writer, long pmapHeader, int bitMask, long value) {
        if (0 != (pmapHeader&bitMask)) {
        	DataOutputBlobWriter.writePackedLong(writer, value);
        }
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
    static void incrementLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (0 != (pmapHeader&bitMask)) {
    		longDictionary[idx]++;
    	}
    		DataOutputBlobWriter.writePackedLong(writer, longDictionary[idx]);
    }
    
    //this method just uses the previous value that was sent
    static void copyLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (0 == (pmapHeader&bitMask)) {
    		DataOutputBlobWriter.writePackedLong(writer, longDictionary[idx]);
    	}
    }
    
    //encodes default value for a long
    static void encodeDefaultLong(long[] defaultLongDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitmask, int idx, long value){
    	if (0 != (pmapHeader & bitmask)){
    		DataOutputBlobWriter.writePackedLong(writer, defaultLongDictionary[idx]);
    	}
    	else{
    		DataOutputBlobWriter.writePackedLong(writer, value);
    	}
    }
    

}
