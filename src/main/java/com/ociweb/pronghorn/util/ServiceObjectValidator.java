package com.ociweb.pronghorn.util;

public interface ServiceObjectValidator<T> {

    /**
     * Returns serviceObject passed in if it is still valid otherwise returns null.
     * 
     * @param serviceObject
     * @return serviceObject
     */
    boolean isValid(T serviceObject);

}
