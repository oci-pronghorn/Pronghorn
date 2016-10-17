package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.util.Random;

/**
 * Created by jake on 10/17/16.
 */
public class RandomReaderStage extends PronghornStage {


    Pipe<MessageSchemaDynamic> input;


    public RandomReaderStage(GraphManager gm, Pipe<MessageSchemaDynamic>  input){
        super(gm,input,NONE);
        this.input = input;
    }

    @Override
    public void run() {
            Random rnd = new Random();
            int random, storeID, amount, recordID, msgID;
            long date;
            String productName, units;
            StringBuilder strProuctName = new StringBuilder();
            StringBuilder strUniits = new StringBuilder();
            while(Pipe.hasContentToRead(input)){
                msgID = Pipe.takeValue(input);
                storeID = Pipe.takeValue(input);
                date = Pipe.takeLong(input);
                strProuctName = Pipe.readASCII(input, strProuctName, Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input));
                amount = Pipe.takeValue(input);
                recordID = Pipe.takeValue(input);
                Pipe.readOptionalASCII(input, strUniits, Pipe.takeRingByteMetaData(input), Pipe.takeRingByteLen(input));
                Pipe.confirmLowLevelRead(input, 11);


                System.out.println("storeID = " + storeID);
                System.out.println("date = " + date);
                System.out.println("ProductName = " + strProuctName.toString());
                System.out.println("amount = " + amount);
                System.out.println("record ID = " + recordID);
                System.out.println("Units = " + strUniits.toString());
            }
    }
}
