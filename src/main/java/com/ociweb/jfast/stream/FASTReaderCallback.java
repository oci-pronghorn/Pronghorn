//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

public interface FASTReaderCallback {

	/*
	 * Implementations of read can use the passed in reader object
	 * to pull the most recent parsed values of any available fields.
	 * Even those in outer groups however values appearing in the template
	 * after the <groupId> will not have been read yet and are not available.
	 * If those values are needed wait until this method is called with the
	 * desired surrounding <groupId>.
	 * 
	 * This callback supports dynamic modification of the templates including:  
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
	void read(int groupId, FASTDynamicReader reader);
	
}
