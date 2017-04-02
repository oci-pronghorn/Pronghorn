package com.ociweb.pronghorn.util.parse;

import java.lang.reflect.Array;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.util.ByteConsumer;
import com.ociweb.pronghorn.util.TrieKeyable;
import com.ociweb.pronghorn.util.TrieParser;

public class JSONStreamVisitorToPipe<T extends MessageSchema, K extends Enum<K> & TrieKeyable> implements JSONStreamVisitor {

	private final FieldReferenceOffsetManager from;
	private final Pipe<T> pipe;
	
	//map array sequence check against field in a message then apply
	
	private final int[] stack;//these are the enum values
	
	private final int UNKNOWN = -1;
	private final int ARRAY   = -2;

	private int stackPosition=0;
	
	public JSONStreamVisitorToPipe(Pipe<T> pipe, Class<K> keys) {
		
		this.from = null==pipe? null :Pipe.from(pipe);
		this.pipe = pipe;
		
		int maxStack = 16;
		stack = new int[maxStack];
		Arrays.fill(stack, UNKNOWN);
		
	}
	
	public void selectField() {
		
		int[] data = Arrays.copyOfRange(stack, 0,  stackPosition);
		
		System.err.println(Arrays.toString(data));
		
		
		//need records with msgId, fieldID, fieldToken
		
		//mask for mach or skip, single int with bits.
		//runs check from smallest up, stop on first mis match
		//array of values to match, check while bits are non zero.
		//single int and short array? with triplet
		//This is really a trie in reverse for matching???
		
		
		
		
		//check all the values in stack to find a match
		//this match gives us the field id and type plus message id
		//copy data for field		
				
	}
	
	
	@Override
	public void nameSeparator() {
	}

	@Override
	public void endObject() {
		stackPosition--;
	}

	@Override
	public void beginObject() {
		stackPosition++;
	}

	@Override
	public void beginArray() {	
		
		stack[stackPosition] = ARRAY;
		stackPosition++;

	}

	@Override
	public void endArray() {
		stackPosition--;
		
	}

	@Override
	public void valueSeparator() {		
		stack[stackPosition-1]--; //this counts down for each index in the array.		
	}

	@Override
	public void whiteSpace(byte b) {		
	}

	@Override
	public void literalTrue() {
		selectField();
		// TODO Auto-generated method stub
		//if this pattern matches
		//now add the value
		
	}

	@Override
	public void literalNull() {
		selectField();
		// TODO Auto-generated method stub
		//if this pattern matches
		//now add the value		
	}

	@Override
	public void literalFalse() {
		selectField();
		// TODO Auto-generated method stub
		//if this pattern matches
		//now add the value
	}

	@Override
	public void numberValue(long m, byte e) {
		selectField();
		// TODO Auto-generated method stub
		//if this pattern matches
		//now add the value
	}

	@Override
	public void stringBegin() {
		selectField();
		// TODO Auto-generated method stub
		
	}

	@Override
	public ByteConsumer stringAccumulator() {
		// TODO Auto-generated method stub
		
		
		
		return null;
	}

	@Override
	public void stringEnd() {
		
		// TODO Auto-generated method stub
		
	}

	@Override
	public void customString(int id) {
		stack[stackPosition] = id;
	}

}
