package com.ociweb.pronghorn.util.math;

import com.ociweb.pronghorn.util.Appendables;

public class ScriptedSchedule {

    public final long commonClock;
    public final byte[] script;
    public final int maxRun;
    
    public ScriptedSchedule(long commonClock, byte[] script, int maxRun) {
        this.commonClock = commonClock;
        this.script = script;
        this.maxRun = maxRun;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        Appendables.appendValue(builder, "Clock:", commonClock, "ns  Script:");
        Appendables.appendArray(builder, '[', script, ']');
                
        return builder.toString();
    }
    

}
