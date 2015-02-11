//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.InputBlockagePolicy;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffers;


// TODO: B, Check support for group that may be optional

/*
 * Implementations of read can use this object
 * to pull the most recent parsed values of any available fields.
 * Even those in outer groups may be read however values appearing in the template
 * after the <groupId> will not have been read yet and are not available.
 * If those values are needed wait until this method is called with the
 * desired surrounding <groupId>.
 * 
 * Supports dynamic modification of the templates including:  
 * 		Field compression/operation type changes.
 * 	    Field order changes within a group.
 *      Mandatory/Optional field designation.
 *      Pulling up fields from group to the surrounding group.
 *      Pushing down fields from group to the internal group.
 * 
 * In some cases after modification the data will no longer be available
 * and unexpected results can occur.  Caution must be used whenever pulling up
 * or pushing down fields as it probably changes the meaning of the data. 
 * 
 */
public final class FASTReaderReactor {

    /**
     * Read up to the end of the next sequence or message (eg. a repeating
     * group)
     * 
     * Rules for making client compatible changes to templates. - Field can be
     * demoted to more general common value before the group. - Field can be
     * promoted to more specific value inside sequence - Field order inside
     * group can change but can not cross sequence boundary. - Group boundaries
     * can be added or removed.
     * 
     * Note nested sequence will stop once for each of the sequences therefore
     * at the bottom hasMore may not have any new data but is only done as a
     * notification that the loop has completed.
     * 
     * @return
     */
    
    private final FASTDecoder decoder; 
    public final PrimitiveReader reader;//the reader is non-blocking but awkward to use directly.
    
    //TODO: B, single execution service must be used for all and passed in, it also needs extra paused threads for release later.
    //TODO: B, reactor will add its runnable to to the single service and remove upon dispose.
    
    public FASTReaderReactor(FASTDecoder decoder, PrimitiveReader reader) {
        this.decoder=decoder;
        this.reader=reader;
        
    }
    
    //TODO: B, support zero copy mapping by (reader adds gaps to rb, writer can skip inputs from rb) add to config
    
    public AtomicBoolean start(final ThreadPoolExecutor executorService, final PrimitiveReader reader) {
        
        final AtomicBoolean isAlive = new AtomicBoolean(true);
        
        reader.setInputPolicy(new InputBlockagePolicy() {
            Object lock = new Object();

            @Override
            public void detectedInputBlockage(int need, FASTInput input) {
                //TODO: C, create these extra threads on startup and pause them until this moment, this prevents creation and gc at runtime
                //TODO: C, formalize this pattern in a new M:N ThreadPoolExecutor, send in the number of cores you wish to target for parsing not threads.
                //TODO: C, once this threading is in place can the move next also be added to the same pool if we desire? This may give us locality across both calls
                //System.err.println("Begin block");
                synchronized(lock) {
                    executorService.setMaximumPoolSize(executorService.getMaximumPoolSize()+1);
                }                
            }
             
            @Override
            public void resolvedInputBlockage(FASTInput input) {
                System.err.println("Release block");
                synchronized(lock) {
                    executorService.setMaximumPoolSize(executorService.getMaximumPoolSize()-1);
                }
            }
            
        });
        
        final Runnable run = buildRunnable(executorService, isAlive);        
        executorService.execute(run);        
        return isAlive;
    }

    private Runnable buildRunnable(final ThreadPoolExecutor executorService, final AtomicBoolean isAlive) {
        final Runnable run = new Runnable() {

            final FASTDecoder decoder2 = FASTReaderReactor.this.decoder;
            final PrimitiveReader reader2 = FASTReaderReactor.this.reader;

            @Override
            public void run() {

                    int f=0;
                    
                    int c = 0x1FFFFF;
                    while (--c>=0)  { 
                        
                        if ((f=decoder2.decode(reader2))<=0) { //break on eof or no room to read
                           
                            break;
                        }  
                    }
                       
                    if (f>=0) {
                        executorService.execute(this);//buildRunnable(executorService,isAlive));
                    } else {
                        isAlive.set(false);
                    }

            }
            
        };
        return run;
    }

    public static int pump(FASTReaderReactor reactor) {
    	//System.err.println("  _______________________  FAST Decode (write to ring buffer )");
            return reactor.decoder.decode(reactor.reader);
    }
    
    public RingBuffer[] ringBuffers() {
    	return RingBuffers.buffers(decoder.ringBuffers);
    }
    

}
