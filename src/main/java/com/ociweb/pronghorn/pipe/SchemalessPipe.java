package com.ociweb.pronghorn.pipe;

public class SchemalessPipe {

	public static void writeFloat(Pipe<MessageSchemaDynamic> p, float value) {
		Pipe.addIntValue(Float.floatToIntBits(value),p);
	}
	
	public static float readFloat(Pipe<MessageSchemaDynamic> p) {
		return Float.intBitsToFloat(Pipe.takeInt(p));
	}
	
	public static void writeInt(Pipe<MessageSchemaDynamic> p, int value) {
		Pipe.addIntValue(value,p);
	}
	
	public static int readInt(Pipe<MessageSchemaDynamic> p) {
		return Pipe.takeInt(p);
	}
	
	public static void writeDouble(Pipe<MessageSchemaDynamic> p, double value) {
		Pipe.addLongValue(Double.doubleToLongBits(value),p);
	}
	
	public static double readDouble(Pipe<MessageSchemaDynamic> p) {
		return Double.longBitsToDouble(Pipe.takeLong(p));
	}
	
	public static void writeLong(Pipe<MessageSchemaDynamic> p, long value) {
		Pipe.addLongValue(value,p);
	}
	
	public static long readLong(Pipe<MessageSchemaDynamic> p) {
		return Pipe.takeLong(p);
	}
	
	public static void publishWrites(Pipe<MessageSchemaDynamic> p) {
		Pipe.publishWritesBatched(p);
	}
	
	public static void releaseReads(Pipe<MessageSchemaDynamic> p) {
		Pipe.releaseReadsBatched(p);
	}
	
	public static int contentRemaining(Pipe<MessageSchemaDynamic> p) {
		return Pipe.contentRemaining(p);
	}
	
	public static int roomRemaining(Pipe<MessageSchemaDynamic> p) {
		return p.sizeOfSlabRing - Pipe.contentRemaining(p);
	}
	
}
