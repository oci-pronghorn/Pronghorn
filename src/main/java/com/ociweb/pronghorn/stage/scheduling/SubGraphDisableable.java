package com.ociweb.pronghorn.stage.scheduling;

public interface SubGraphDisableable {

	void disable(int idx, boolean b);

	boolean disabled(int idx);

}
