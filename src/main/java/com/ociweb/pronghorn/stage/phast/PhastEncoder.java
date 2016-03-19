package com.ociweb.pronghorn.stage.phast;

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
        if (0 == (pmapHeader&bitMask)) {
            DataOutputBlobWriter.writePackedInt(writer, value-intDictionary[idx]);
            intDictionary[idx] = value;
        }
    }

    
    static void encodeDeltaLong(long[] longDictionary, DataOutputBlobWriter writer, long pmapHeader, int idx, int bitMask, long value) {
        if (0 == (pmapHeader&bitMask)) {
            
            DataOutputBlobWriter.writePackedLong(writer, value-longDictionary[idx]);
            longDictionary[idx] = value;             
        }
    }
    
    //this method encodes a string
    static void encodeString(DataOutputBlobWriter writer, String value) throws UnsupportedEncodingException{
    	//encode -63 so it knows it is variable length
    	DataOutputBlobWriter.writePackedUInt(writer, -63);
    		 
    	//calculate string length in bytes, then encode it
    	byte[] byteArray = value.getBytes("UTF-16BE");
    	DataOutputBlobWriter.writePackedInt(writer, byteArray.length);
    	DataOutputBlobWriter.writePackedChars(writer, value);
    }
    
    //this method increments a dictionary value by one, then writes it to the pipe
    static void incrementInt(int[] intDictionary, DataOutputBlobWriter writer, long pmapHeader, int bitMask, int idx){
    	if (0 == (pmapHeader&bitMask)) {
    		intDictionary[idx]++;
    		DataOutputBlobWriter.writePackedInt(writer, intDictionary[idx]);
        
    	}
    }

}
