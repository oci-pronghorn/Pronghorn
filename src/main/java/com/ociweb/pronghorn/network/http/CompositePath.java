package com.ociweb.pronghorn.network.http;

public interface CompositePath extends CompositeRoute {

	//NOTE: if path does not start with / then we add it with no complaint.
	CompositePath path(CharSequence path);
	
}
