package com.ociweb.pronghorn.pipe.build;
import com.ociweb.pronghorn.pipe.stream.LowLevelStateManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
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
private long[] longDictionary;
private int[] intDictionary;
DataInputBlobReader<MessageSchemaDynamic> reader = new DataInputBlobReader<MessageSchemaDynamic>(output);
public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
    new int[]{0xc1400007,0x88200000,0x98800000,0xa0000000,0x80800001,0x80a00002,0xa4000001,0xc1200007},
    (short)0,
    new String[]{"InventoryDetails","StoreID","Date","ProductName","Amount","RecordID","Units",null},
    new long[]{1, 101, 102, 103, 104, 105, 106, 0},
    new String[]{"global",null,null,null,null,null,null,null},
    "groceryExample.xml",
    new long[]{2, 2, 0},
    new int[]{2, 2, 0});
private final int[] FROM_GUID = new int[]{236463696, 1042588431, 307749989, 0, (-1421281399), (-1920029437), (-261718140), 1608417298};
private final long BUILD_TIME = 1473168503184L;
private static final int DO_NOTHING = -3;

private int nextMessageIdx() {
    return 0;
}


public void startup(){
intDictionary = FROM.newIntDefaultsDictionary();
longDictionary = FROM.newLongDefaultsDictionary();
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
    long bitMask = 1;
    long map = DataInputBlobReader.readPackedLong(reader);
    StoreID = PhastDecoder.decodeCopyInt(intDictionary, reader, map, 1, bitMask);
    bitMask = bitMask << 1;
    Date = PhastDecoder.decodeDeltaLong(longDictionary, reader, map, 2, bitMask);
    bitMask = bitMask << 1;
    ProductName = PhastDecoder.decodeString(reader);
    bitMask = bitMask << 1;
    Amount = PhastDecoder.decodeDeltaInt(intDictionary, reader, map, 100, bitMask, );
    bitMask = bitMask << 1;
    RecordID = PhastDecoder.decodeIncrementInt(intDictionary, map, 5, bitMask);
    bitMask = bitMask << 1;
    Units = PhastDecoder.decodeString(reader);
    bitMask = bitMask << 1;
    processPipe1WriteInventoryDetails(StoreID,Date,ProductName,Amount,RecordID,Units);
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
