package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.util.Random;

/**
 * Created by jake on 10/17/16.
 */
public class GroceryFuzzGenerator extends PronghornStage{


    Pipe<MessageSchemaDynamic>  output;
    private int i;
    private Random rnd;

    public GroceryFuzzGenerator(GraphManager gm, Pipe<MessageSchemaDynamic>  output){
        super(gm,NONE,output);
        this.output = output;
        i=0;
    }

    @Override
    public void startup(){
        rnd = new Random();
    }

    @Override
    public void run() {

        while (Pipe.hasRoomForWrite(output)) {
            if (++i<10){
                int random, storeID, amount, recordID;
                long date;
                String productName, units;

                random = rnd.nextInt(50000);
                storeID = rnd.nextInt(50000);
                date = (long) rnd.nextInt(50000);
                productName = "first string test " + Integer.toString(rnd.nextInt(50000));
                amount = rnd.nextInt(50000);
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
                Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(output, 0));
                Pipe.publishWrites(output);

                //System.out.println("id : " + storeID + " Date " + date + " Product Name " + productName + " Amounnt " + amount + "Units " + units);
            }
            else {
                Pipe.spinBlockForRoom(output, Pipe.EOF_SIZE);
                Pipe.publishEOF(output);
                requestShutdown();
                return;
            }
        }
    }

}
