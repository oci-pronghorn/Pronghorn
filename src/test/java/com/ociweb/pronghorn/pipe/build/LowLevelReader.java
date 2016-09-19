package com.ociweb.pronghorn.pipe.build;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.stage.phast.PhastEncoder;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;

public class LowLevelReader implements Runnable {

private void requestShutdown() {};
private StringBuilder workspace0x67;
private StringBuilder workspace0x6a;
private Pipe<MessageSchemaDynamic> input;
public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
    new int[]{0xc1400007,0x88200000,0x98800000,0xa0000000,0x80800001,0x80a00002,0xa4000001,0xc1200007},
    (short)0,
    new String[]{"InventoryDetails","StoreID","Date","ProductName","Amount","RecordID","Units",null},
    new long[]{1, 101, 102, 103, 104, 105, 106, 0},
    new String[]{"global",null,null,null,null,null,null,null},
    "groceryExample.xml",
    new long[]{2, 2, 0},
    new int[]{2, 2, 0});
long[] previousLongDictionary;
int[] previousIntDictionary;
long[] defLongDictionary;
int[] defIntDictionary;

// GENERATED LOW LEVEL READER 
// # Low level API is the fastest way of reading from a pipe in a business semantic way. 
// # Do not change the order that fields are read, this is fixed when using low level. 
// # Do not remove any field reading, every field must be consumed when using low level. 

// Details to keep in mind when you expect the schema to change over time
// # Low level API is CAN be extensiable in the sense which means ignore unrecognized messages. 
// # Low level API is CAN NOT be extensiable in the sense of dealing with mising or extra/new fields. 
// # Low level API is CAN NOT be extensiable in the sense of dealing with fields encoded with different types. 
private static final int[] FROM_GUID = new int[]{236463696, 1042588431, 307749989, 0, (-1421281399), (-1920029437), (-261718140), 1608417298};
private static final long BUILD_TIME = 1474295683194L;

public void startup() {
    Pipe.from(input).validateGUID(FROM_GUID);

    previousIntDictionary = FROM.newIntDefaultsDictionary();
    previousLongDictionary = FROM.newLongDefaultsDictionary();
    defIntDictionary = FROM.newIntDefaultsDictionary();
    defLongDictionary = FROM.newLongDefaultsDictionary();
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
            Pipe.readOptionalASCII(input, Appendables.truncate(this.workspace0x6a), Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input))    );
}

protected void businessMethodInventoryDetails(int pStoreID, long pDate, StringBuilder workspace0x67, int pAmount, int pRecordID, StringBuilder workspace0x6a) {
DataOutputBlobWriter<MessageSchemaDynamic> writer = new DataOutputBlobWriter<MessageSchemaDynamic>(input);
    long map = 0;
    map = PhastEncoder.pmapBuilderString(map, 0x88200000, (workspace0x6a == null));
    map = PhastEncoder.pmapBuilderInt(map, 0x98800000, pRecordID, previousIntDictionary[4], defIntDictionary[4], false);
    map = PhastEncoder.pmapBuilderInt(map, 0xa0000000, pAmount, previousIntDictionary[3], defIntDictionary[3], false);
    map = PhastEncoder.pmapBuilderString(map, 0x80800001, (workspace0x67 == null));
    map = PhastEncoder.pmapBuilderLong(map, 0x80a00002, pDate, previousLongDictionary[1], defLongDictionary[1], false);
    map = PhastEncoder.pmapBuilderInt(map, 0xa4000001, pStoreID, previousIntDictionary[0], defIntDictionary[0], true);
    DataOutputBlobWriter.writePackedLong(writer, map);
    long bitMask = 1;

    PhastEncoder.copyInt(previousIntDictionary, writer, map, bitMask, 0, pStoreID);
    bitMask = bitMask << 1;
    PhastEncoder.encodeDeltaLong(previousLongDictionary, writer, map, bitMask, 1, pDate);
    bitMask = bitMask << 1;
    PhastEncoder.encodeString(writer, workspace0x67, map, bitMask);
    bitMask = bitMask << 1;
    PhastEncoder.encodeDeltaInt(previousIntDictionary, writer, map, bitMask, 3, pAmount);
    bitMask = bitMask << 1;
    PhastEncoder.incrementInt(previousIntDictionary, writer, map, bitMask, 4);
    bitMask = bitMask << 1;
    PhastEncoder.encodeString(writer, workspace0x6a, map, bitMask);
}

}


