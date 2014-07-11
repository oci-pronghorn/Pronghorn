package com.ociweb.jfast.util;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Profile {

    public static AtomicInteger version = new AtomicInteger();
    
    public static long count;
    
    public static Executor  ex = Executors.newSingleThreadExecutor();
    
    //thread loop to inc version.
    public static int tmp;
    
    //at method star
    // pVer = Profile.version;
    
    //at method end
    // Profile.count += (Profile.version-pVer);
    
    
    public static void start() {
        
        ex.execute(new Runnable() {
           
            @Override
            public void run() {
                while (true) {
                    
                    try {
                        Thread.sleep(7);
                    } catch (InterruptedException e) {
                        return;
                    }
                    version.incrementAndGet();
                }
            }
            
        });
        
    }
    
    
    
    public static String results() {
        
        return "cycles :"+version+" count :"+count+" pct "+(count/(float)version.intValue());
        
    }
    
    
}
