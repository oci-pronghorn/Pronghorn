package com.ociweb.pronghorn.util.math;

public class ScriptedSchedule {

    //package protected
    final int commonClock;
    final byte[] script;
    
    
    public ScriptedSchedule(int commonClock, byte[] script) {
        this.commonClock = commonClock;
        this.script = script;
    }
    

}
