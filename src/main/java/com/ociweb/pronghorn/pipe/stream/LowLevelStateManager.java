package com.ociweb.pronghorn.pipe.stream;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;

public class LowLevelStateManager {
    final int[] cursorStack;
    private final int[] sequenceCounters;
    int nestedFragmentDepth;
    private FieldReferenceOffsetManager from;

    public LowLevelStateManager(Pipe inputRing) {
        this.from = Pipe.from(inputRing);
        this.cursorStack = new int[from.maximumFragmentStackDepth];
        this.sequenceCounters = new int[from.maximumFragmentStackDepth];        

        //publish only happens on fragment boundary therefore we can assume that if 
        //we can read 1 then we can read the full fragment
        
        this.nestedFragmentDepth = -1; 
    }

    public static void processGroupLength(LowLevelStateManager that, final int cursor, int seqLen) {
        that.nestedFragmentDepth++;
        that.sequenceCounters[that.nestedFragmentDepth]= seqLen;
        that.cursorStack[that.nestedFragmentDepth] = cursor+that.from.fragScriptSize[cursor];
    }

    public static int activeCursor(LowLevelStateManager that) {
        return that.cursorStack[that.nestedFragmentDepth];
    }

    public static boolean isStartNewMessage(LowLevelStateManager that) {
        return that.nestedFragmentDepth<0;
    }

    public static int closeFragment(LowLevelStateManager that) {
        return that.nestedFragmentDepth--;
    }

    public static boolean closeSequenceIteration(LowLevelStateManager that) {
        return --that.sequenceCounters[that.nestedFragmentDepth]<=0;
    }

    public static void continueAtThisCursor(LowLevelStateManager that, int fieldCursor) {
        that.cursorStack[++that.nestedFragmentDepth] = fieldCursor;
    }
}