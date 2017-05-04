package com.ociweb.pronghorn.stage.filter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.RollingBloomFilter;

public class PassRepeatsFilterStage<T extends MessageSchema<T>> extends PronghornStage {

    //after instances of each id then let them pass
    //saves its own state
    
    private final int maximumItems = 1_000_000;
    private final double maximumFailure = .0001; 
    // 10_000_000  .00001 -> 32MB
    
    private int instances;
    private RollingBloomFilter[] filters;
    private final Pipe<T> input;
    private final Pipe<T> output;
    private final int varFieldLoc;
    private final File storage;
    private final File backup;
 
    public PassRepeatsFilterStage(GraphManager graphManager, Pipe<T> input, Pipe<T> output, int instances, int varFieldLoc, File storage) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
        this.instances = instances;
        this.varFieldLoc = varFieldLoc;
        this.storage = storage;
        try {
            this.backup = new File(storage.getCanonicalPath()+".bak");
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
    }

    @Override
    public void startup() {
   
        if (storage.exists()) {
            try {
                FileInputStream fist = new FileInputStream(storage);
                ObjectInputStream oist = new ObjectInputStream(fist);                
                filters = (RollingBloomFilter[]) oist.readObject();
                instances = filters.length;
                oist.close();
            } catch (Throwable e) {
                System.out.println("build new repeats filter, old one is not compatible");
                buildNewFilters();
            }
            
//            System.out.println("loaded BloomFilters");
//            int i = filters.length;
//            while (--i>=0) {
//                System.out.println(i+" pct full "+ (100f*filters[i].pctConsumed()));
//            }
//            System.out.println();
            
            
        } else {
            buildNewFilters();
        }
    }

    private void buildNewFilters() {
        int i = instances;
        filters = new RollingBloomFilter[i];
        while (--i>=0) {
            filters[i] = new RollingBloomFilter(maximumItems, maximumFailure);
        }
    }
    
    @Override
    public void shutdown() {
        //NOTE: this save resume logic is all new and may be replaced with pipes and/or and external system.
        
        saveFilter();
    }

    private void saveFilter() {
        try {
            if (backup.exists()) {
                backup.delete();
            }
            storage.renameTo(backup);
            
            FileOutputStream fost = new FileOutputStream(storage);
            ObjectOutputStream oost = new ObjectOutputStream(fost);
            oost.writeObject(filters);
            oost.close();
            
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }
    
    boolean moveInProgress = false;
    
    @Override
    public void run() {
        if (moveInProgress) {
            if (!PipeReader.tryMoveSingleMessage(input, output)) {
                return;
            } else {
                moveInProgress = false;
                PipeReader.releaseReadLock(input);
            }
        }
        
        boolean updateFile = false;
        while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
            updateFile = true;            
            byte[] backing = PipeReader.readBytesBackingArray(input, varFieldLoc);
            int pos = PipeReader.readBytesPosition(input, varFieldLoc);
            int len = PipeReader.readBytesLength(input, varFieldLoc);
            int mask = PipeReader.readBytesMask(input, varFieldLoc);
            
            int i = instances;
            while (--i>=0 && filters[i].mayContain(backing,pos,len,mask)) {
            }
            
            if (i<0) {
                if (!PipeReader.tryMoveSingleMessage(input, output)) {
                     moveInProgress = true;
                    return;
                }
            } else {
                //this one did not contain it so add it.
                filters[i].addValue(backing,pos,len,mask);
            }
            
            PipeReader.releaseReadLock(input);
        }
        if (updateFile) {
            saveFilter();
        }
    }   
    

}
