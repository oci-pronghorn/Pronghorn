package com.ociweb.pronghorn.pipe.build;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public class LowLevelReader implements Runnable {

private void requestShutdown() {};
private StringBuilder workspace0x67;
private StringBuilder workspace0x6A;
private String Units;
private int RecordID;
private int Amount;
private String ProductName;
private long Date;
private int StoreID;
private Pipe<MessageSchemaDynamic> input;

// GENERATED LOW LEVEL READER 
// # Low level API is the fastest way of reading from a pipe in a business semantic way. 
// # Do not change the order that fields are read, this is fixed when using low level. 
// # Do not remove any field reading, every field must be consumed when using low level. 

// Details to keep in mind when you expect the schema to change over time
// # Low level API is CAN be extensiable in the sense which means ignore unrecognized messages. 
// # Low level API is CAN NOT be extensiable in the sense of dealing with mising or extra/new fields. 
// # Low level API is CAN NOT be extensiable in the sense of dealing with fields encoded with different types. 
private static final int[] FROM_GUID = new int[]{236463696, 1042588431, 307749989, 0, (-1421281399), (-1920029437), (-261718140), 1608417298};
private static final long BUILD_TIME = 1468553452144L;

public void startup() {
    Pipe.from(input).validateGUID(FROM_GUID);
}

@Override
public void run() {
    if (!Pipe.hasContentToRead(input)) {
        return;
    }
    int cursor;
if (Pipe.hasContentToRead(input)) {
cursor = Pipe.takeMsgIdx(input);
    switch(cursor) {
        case /*processPipeInventoryDetails*/0:
            processPipeInventoryDetails();
            Pipe.confirmLowLevelRead(input, 11 /* fragment size */);
        break;
        case -1:    Pipe.takeMsgIdx(input);
    Pipe.takeValue(input);
        requestShutdown();
    break;
        default:
            throw new UnsupportedOperationException("Unknown message type, rebuid with the new schema.");
    }
    Pipe.releaseReads(input);
}
}

private void processPipeInventoryDetails() {
    businessMethodInventoryDetails(
            Pipe.takeValue(input),
            Pipe.takeLong(input),
            Pipe.readASCII(input, Appendables.truncate(this.workspace0x67), Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input)),
            Pipe.takeValue(input),
            Pipe.takeValue(input),
            Pipe.readOptionalASCII(input, Appendables.truncate(this.workspace0x6A), Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input))    );
}

protected void businessMethodInventoryDetails(int pStoreID, long pDate, StringBuilder workspace0x67, int pAmount, int pRecordID, StringBuilder workspace0x6A) {
DataOutputBlobWriter<MessageSchemaDynamic> input = new DataOutputBlobWriter<MessageSchemaDynamic>(input);
    int[] previousIntDictionary = new int[5];
    long[] previousLongDictionary = new long[5];
    int[] defIntDictionary = new int[5];
    long[] defLongDictionary = new long[5];
    long map = 0;
    map = PhastEncoder.pmapBuilderInt(map, -2011168768, pStoreID, previousIntDictionary[0], defIntDictionary[0], false);
    map = PhastEncoder.pmapBuilderLong(map, -1736441856, pDate, previousLongDictionary[1], defLongDictionary[1], false);
    map = PhastEncoder.pmapBuilderString(map, -1610612736, (workspace0x67 == null));
    map = PhastEncoder.pmapBuilderInt(map, -2139095039, pAmount, previousIntDictionary[3], defIntDictionary[3], false);
    map = PhastEncoder.pmapBuilderInt(map, -2136997886, pRecordID, previousIntDictionary[4], defIntDictionary[4], false);
    map = PhastEncoder.pmapBuilderString(map, -1543503871, (workspace0x6A == null));
    DataOutputBlobWriter.writePackedLong(input, map);
    long bitMask = 1;
    bitMask = bitMask << 5;

    PhastEncoder.copyInt(previousIntDictionary, input, map, bitMask, 0, pStoreID);
    bitMask = bitMask >> 1;
    PhastEncoder.encodeDeltaLong(previousLongDictionary, input, map, bitMask, 1, pDate);
    bitMask = bitMask >> 1;
    PhastEncoder.encodeString(input, workspace0x67, map, bitMask);
    bitMask = bitMask >> 1;
    PhastEncoder.encodeDeltaInt(previousIntDictionary, input, map, bitMask, 3, pAmount);
    bitMask = bitMask >> 1;
    PhastEncoder.incrementInt(previousIntDictionary, input, map, bitMask, 4);
    bitMask = bitMask >> 1;
    PhastEncoder.encodeString(input, workspace0x6A, map, bitMask);
}

}


