package com.ociweb.pronghorn.pipe.build;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.stage.phast.PhastDecoder;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
public class LowLevelWriter implements Runnable {

private Pipe<MessageSchemaDynamic> output;
private String Units;
private int RecordID;
private int Amount;
private String ProductName;
private long Date;
private int StoreID;
private final int[] FROM_GUID = new int[]{236463696, 1042588431, 307749989, 0, (-1421281399), (-1920029437), (-261718140), 1608417298};
private final long BUILD_TIME = 1471636213551L;
private static final int DO_NOTHING = -3;

private int nextMessageIdx() {
    return 0;
}

@Override
public void run() {
    while (Pipe.hasRoomForWrite(output)) {
    switch(nextMessageIdx()) {
        case /*processPipe1WriteInventoryDetails*/0:
        Pipe.addMsgIdx(output,0);
            processInventoryDetails();
            Pipe.confirmLowLevelWrite(output, 11/* fragment 0  size 8*/);
        break;
        case DO_NOTHING:
            return;
        default:
            throw new UnsupportedOperationException("Unknown message type , rebuid with the new schema.");
    }
    Pipe.publishWrites(output);
    }
}

private void processInventoryDetails() {
    long map = DataInputBlobReader.readPackedLong(reader);
    StoreID = PhastDecoder.decodeCopyInt(intDictionary, reader, map, 1, bitMask);
    Date = PhastDecoder.decodeDeltaLong(longDictionary, reader, map, 2, bitMask);
    ProductName = PhastDecoder.decodeString(reader);
    Amount = PhastDecoder.decodeDeltaInt(intDictionary, reader, map, 100, bitMask, intVal);
    RecordID = PhastDecoder.decodeIncrementInt(intDictionary, map, 5, bitMask);
    Units = PhastDecoder.decodeString(reader);
}

private void processPipe1WriteInventoryDetails( int pStoreID,long pDate,CharSequence pProductName,int pAmount,int pRecordID,CharSequence pUnits) {
    Pipe.addIntValue(pStoreID,output);
    Pipe.addLongValue(pDate,output);
    Pipe.addASCII(pProductName,output);
    Pipe.addIntValue(pAmount,output);
    Pipe.addIntValue(pRecordID,output);
    Pipe.addASCII(pUnits,output);
}

private void requestShutdown() {};
};
