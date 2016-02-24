package com.ociweb.pronghorn.stage.phast;

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

}
