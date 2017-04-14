package com.ociweb.pronghorn.stage.filter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

public class PassUniquesFilterStage<T extends MessageSchema> extends PronghornStage {

    //after instances of each id then let them pass
    //saves its own state
    
    private final int maximumItems = 10_000_000;
    private final double maximumFailure = .00001; 
    // 10_000_000  .00001 -> 32MB
    
    private RollingBloomFilter filter;
    private final Pipe<T> input;
    private final Pipe<T> output;
    private final int varFieldLoc;
    private final File storage;
    private final File backup;
    private boolean moveInProgress = false;            
    
    public PassUniquesFilterStage(GraphManager graphManager, Pipe<T> input, Pipe<T> output, int varFieldLoc, File storage) {
        super(graphManager, input, output);
        this.input = input;
        this.output = output;
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
            File localStorage = storage;
            
            try {
                filter = loadFilter(localStorage);
            } catch (Exception e) {
                System.out.println("Unable to load old filters, starting with new files");
                buildNewFilters();
            }
            
            System.out.println("loaded BloomFilter");
            System.out.println("seen pct full "+ (100f*filter.pctConsumed()));
            System.out.println();            
            
        } else {
            buildNewFilters();
        }
    }

    private RollingBloomFilter loadFilter(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
        FileInputStream fist = new FileInputStream(file);
        ObjectInputStream oist = new ObjectInputStream(fist);                
        RollingBloomFilter localFilter = (RollingBloomFilter) oist.readObject();
         
        oist.close();
        return localFilter;
    }

    private void buildNewFilters() {
        filter = new RollingBloomFilter(maximumItems, maximumFailure);
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
            oost.writeObject(filter);
            oost.close();
            
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

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
                        
            if (!filter.mayContain(backing,pos,len,mask)) {
                filter.addValue(backing,pos,len,mask);                

                if (! PipeReader.tryMoveSingleMessage(input, output)) {
                    moveInProgress = true;
                    return;
                }
                
            }
            
            PipeReader.releaseReadLock(input);
        }
        if (updateFile) {
            saveFilter();
        }
    }   
    

}
