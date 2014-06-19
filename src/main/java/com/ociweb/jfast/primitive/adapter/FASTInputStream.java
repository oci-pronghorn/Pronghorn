//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive.adapter;

import java.io.IOException;
import java.io.InputStream;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTInput;

public class FASTInputStream implements FASTInput {

	private InputStream inst;
	private byte[] targetBuffer;
	private boolean eof = false;
	private long total;
	
	public FASTInputStream(InputStream inst) {
		this.inst = inst;
	}
	
	public void replaceStream(InputStream inst) {
		this.inst = inst;
	}
	
	public int fill(int offset, int len) {
		try {
			
		    int avail = inst.available();
		    if (avail>0) {
		        //   len = len<avail?len:avail;
		        len = len + ((avail-len) & (avail-len)>>31);//branchless
		    }
					    
			//Only fill with the bytes avail.			
			int result = inst.read(targetBuffer, offset, len);
			if (result<0) {
				eof = true;;
				return 0;
			}
			total+=result;
			return result;
		} catch (IOException e) {
			throw new FASTException(e);
		}
	}

	@Override
	public void init(byte[] targetBuffer) {
		this.targetBuffer = targetBuffer;
	}

	@Override
	public boolean isEOF() {
		return eof;
	}
	
	public long totalBytes() {
	    return total;
	}

    @Override
    public int blockingFill(int offset, int count) {
        int result;
        try {
            result = inst.read(targetBuffer, offset, count);
        } catch (IOException e) {
            throw new FASTException(e);
        }
        if (result<0) {
            eof = true;;
            return 0;
        }
        total+=result;
        return result;
    }
	
}
