package com.ociweb.pronghorn.util.math;

import java.io.IOException;

import com.ociweb.pronghorn.util.Appendables;

public class ScriptedSchedule {

    //package protected
    final int commonClock;
    final byte[] script;
    
    
    public ScriptedSchedule(int commonClock, byte[] script) {
        this.commonClock = commonClock;
        this.script = script;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        
        try {
        
            Appendables.appendValue(builder, "Clock:", commonClock, "ms  Script:");
            Appendables.appendArray(builder, '[', script, ']');
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
                
        return builder.toString();
    }
    

}
