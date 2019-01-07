package com.ociweb.pronghorn.network.http;

public interface ErrorReporter {

	boolean sendError(long id, int errorCode);

}
