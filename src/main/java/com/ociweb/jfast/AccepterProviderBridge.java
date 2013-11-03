package com.ociweb.jfast;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AccepterProviderBridge implements FASTAccept, FASTProvide {
	
	//provider must consume each of the accepted values.
	//accept can not write until the present value is picked up or data may be lost
	
	//***********NEW FEATURES THAT REQUIRE BOTH TEMPLATES PASSED IN ON CONSTRUTION********
	//TODO: provide field names from provider side, if the id in accept does not match drop it. use AcceptFilter
	//TODO: if provider needs fields never produced by accept must throw exception of some sort.
	
	
	// -1 indicates we have never received one.

	int lastNullId = -1;
	Lock lastNullLock = new ReentrantLock();
	
	long lastLong;
	int lastLongId = -1;
	Lock lastLongLock = new ReentrantLock();
	
	int lastInt;
	int lastIntId = -1;
	Lock lastIntLock = new ReentrantLock();
	
	DecimalDTO lastDecimal = new DecimalDTO();
	int lastDecimalId;
	Lock lastDecimalLock = new ReentrantLock();
	
	CharSequence lastCharSequence;
	int lastCharSequenceId = -1;
	Lock lastCharSequenceLock = new ReentrantLock();
	
	byte[] lastBytes;
	int lastBytesId = -1;
	Lock lastBytesLock = new ReentrantLock();
	
	public void accept(int id, long value) {
		lastLongLock.lock();
		lastLongId = id;
		lastLong = value;
	}

	public void accept(int id, int value) {
		lastIntLock.lock();
		lastIntId = id;
		lastInt = value;
	}

	public void accept(int id, int exponent, long manissa) {
		lastDecimalLock.lock();
		lastDecimalId = id;
		lastDecimal.exponent = exponent;
		lastDecimal.mantissa = manissa;
	}

	public void accept(int id, byte[] buffer, int offset, int length) {
		lastBytesLock.lock();
		lastBytesId = id;
		lastBytes = buffer;
	}

	public void accept(int id, CharSequence value) {
		lastCharSequenceLock.lock();
		lastCharSequenceId = id;
		lastCharSequence = value;
	}

	public void accept(int id) {
		lastNullLock.lock();
		lastNullId = id;
	}
		
	
	public boolean provideNull(int id) {
		try{
			return id == lastNullId;
		} finally {
			lastNullLock.unlock();
		}
	}

	public long provideLong(int id) {
		try {
			assert(id == lastLongId);
			return lastLong;
		} finally {
			lastLongLock.unlock();
		}
	}

	public int provideInt(int id) {
		try {
			assert(id == lastIntId);
			return lastInt;
		} finally {
			lastIntLock.unlock();
		}
	}

	public byte[] provideBytes(int id) {
		try {
			assert(id == lastBytesId);
			return lastBytes;
		} finally {
			lastBytesLock.unlock();
		}
	}

	public CharSequence provideCharSequence(int id) {
		try {
			assert(id == lastCharSequenceId);
			return lastCharSequence;
		} finally {
			lastCharSequenceLock.unlock();
		}
	}

	public void provideDecimal(int id, DecimalDTO target) {
		try {
			assert(id == lastDecimalId);
			target.exponent = lastDecimal.exponent;
			target.mantissa = lastDecimal.mantissa;
		} finally {
			lastDecimalLock.unlock();
		}
	}

}
