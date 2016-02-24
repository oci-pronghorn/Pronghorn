package com.ociweb.pronghorn.stage.phast;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;

public class PhastDecoder {

    static long decodeDeltaLong(long[] longDictionary, DataInputBlobReader reader, long map, int idx, long defaultValue, int bitMask) {
        return (0==(map&bitMask)) ? (longDictionary[idx] += DataInputBlobReader.readPackedLong(reader)) : defaultValue;        
    }

    static int decodeDefaultInt(DataInputBlobReader reader, long map, int defaultValue, int bitMask) {
       return (0==(map&bitMask)) ? defaultValue : DataInputBlobReader.readPackedInt(reader);
    }

    static int decodeDeltaInt(int[] intDictionary, DataInputBlobReader reader, long map, int idx, int defaultValue, int bitMask) {
        return (0==(map&bitMask)) ? (intDictionary[idx] += DataInputBlobReader.readPackedInt(reader)) : defaultValue;
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
    
}
