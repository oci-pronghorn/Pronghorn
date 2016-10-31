package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.pipe.MessageSchemaDynamic;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.util.Random;

/**
 * Created by jake on 10/31/16.
 */
public class IntegrityFuzzGenerator extends PronghornStage{


    Pipe<MessageSchemaDynamic> output;
    private int i;
    private Random rnd;

    int [] intLastValues;
    long[] longLastValues;

    public IntegrityFuzzGenerator(GraphManager gm, Pipe<MessageSchemaDynamic>  output){
        super(gm,NONE,output);
        this.output = output;
    }

    @Override
    public void startup(){
        rnd = new Random(1234);
        i = 0;
        intLastValues = new int[3];
        longLastValues = new long[3];
    }

    @Override
    public void run() {

        while (Pipe.hasRoomForWrite(output)) {
            if (++i < 5){
                int deltaInt, incrementInt, copyInt, defaultInt;
                long deltaLong, incrementLong, copyLong, defaultLong;
                String optionalString, noneString;

                //test all ints
                intLastValues[0] = deltaInt = (rnd.nextInt(50000) < 25000)? rnd.nextInt(50000) : intLastValues[0];
                intLastValues[1] = incrementInt = (rnd.nextInt(50000) < 25000)? intLastValues[1] : ++intLastValues[1];
                intLastValues[2] = copyInt = (rnd.nextInt(50000) < 25000)? rnd.nextInt(50000) : intLastValues[2];
                defaultInt = (rnd.nextInt(50000) < 25000)? rnd.nextInt(50000) : 122;

                //test all longs
                longLastValues[0] = deltaLong = (rnd.nextInt(50000) < 25000)? rnd.nextInt(50000) : longLastValues[0];
                longLastValues[1] = incrementLong = (rnd.nextInt(50000) < 25000)? longLastValues[1] : ++longLastValues[1];
                longLastValues[2] = copyLong = (rnd.nextInt(50000) < 25000)? rnd.nextInt(50000) : longLastValues[2];
                defaultLong = (rnd.nextInt(50000) < 25000)? rnd.nextInt(50000) : 122;

                //testStrings
                optionalString = "this is the optional";
                noneString = "this is the none string";

                //put message
                Pipe.addMsgIdx(output, 0);
                //place them on the pipe
                Pipe.addIntValue(deltaInt, output);
                Pipe.addIntValue(incrementInt, output);
                Pipe.addIntValue(copyInt, output);
                Pipe.addIntValue(defaultInt, output);

                Pipe.addLongValue(deltaLong, output);
                Pipe.addLongValue(incrementLong, output);
                Pipe.addLongValue(copyLong, output);
                Pipe.addLongValue(defaultLong, output);

                Pipe.addASCII(optionalString, output);
                Pipe.addASCII(noneString, output);

                Pipe.confirmLowLevelWrite(output, 18);
                Pipe.publishWrites(output);

                System.out.println("Ints: " + deltaInt + ", " + incrementInt + ", " + copyInt + ", " + defaultInt + "\n" +
                                    "Longs: " + deltaLong + ", " + incrementLong + ", " + copyLong + ", " + defaultLong);
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
