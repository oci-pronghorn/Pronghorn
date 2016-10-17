package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.util.Random;

/**
 * Created by jake on 10/17/16.
 */
public class RandomeReaderStage extends PronghornStage {


    Pipe<MessageSchemaDynamic> input;


    public RandomeReaderStage(GraphManager gm, Pipe<MessageSchemaDynamic>  input){
        super(gm,input,NONE);
        this.input = input;
    }

    @Override
    public void run() {
        while (Pipe.hasContentToRead(input)){
            Random rnd = new Random();
            int random, storeID, amount, recordID, msgID;
            long date;
            String productName, units;
            StringBuilder strProuctName = new StringBuilder();
            StringBuilder strUniits = new StringBuilder();
            while (true){
                msgID = Pipe.takeValue(input);
                storeID = Pipe.takeValue(input);
                date = Pipe.takeLong(input);
                strProuctName = Pipe.readASCII(input, strProuctName, Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input));
                amount = Pipe.takeValue(input);
                recordID = Pipe.takeValue(input);
                Pipe.readOptionalASCII(input, strUniits, Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input));
                Pipe.confirmLowLevelRead(input, 11);
            }
        }
    }
}
