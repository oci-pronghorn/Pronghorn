package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.util.Random;

/**
 * Created by jake on 10/17/16.
 */
public class RandomWriterGeneratorStage extends PronghornStage{


    Pipe<MessageSchemaDynamic>  output;


    public RandomWriterGeneratorStage(GraphManager gm, Pipe<MessageSchemaDynamic>  output){
        super(gm,NONE,output);
        this.output = output;
    }

    @Override
    public void run() {
            Random rnd = new Random();
            int random, storeID, amount, recordID;
            long date;
            String productName, units;
            for (int i = 0; i < 10; i++){

                if (Pipe.hasRoomForWrite(output)) {
                    //generate random numbers
                    random = rnd.nextInt(50000);
                    storeID = rnd.nextInt(50000);
                    date = (long) rnd.nextInt(50000);
                    productName = "first string test " + Integer.toString(rnd.nextInt(50000));
                    amount = random * 100;
                    recordID = i;
                    units = "second string test " + Integer.toString(rnd.nextInt(50000));
                    //put message
                    Pipe.addMsgIdx(output, 0);
                    //place them on the pipe
                    Pipe.addIntValue(storeID, output);
                    Pipe.addLongValue(date, output);
                    Pipe.addASCII(productName, output);
                    Pipe.addIntValue(amount, output);
                    Pipe.addIntValue(recordID, output);
                    Pipe.addASCII(units, output);
                    Pipe.confirmLowLevelWrite(output, 11);
                    Pipe.publishWrites(output);
                }
            }

            Pipe.publishEOF(output);
            
    }

}
