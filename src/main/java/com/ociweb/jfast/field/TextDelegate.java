package com.ociweb.jfast.field;

import java.io.IOException;

import com.ociweb.jfast.error.FASTException;


/**
 * Replacement for String field in Objects.  This class attempts to implement all
 * the needed functionality while eliminating any garbage production.  This class
 * is backed by the FAST data dictionary and so eliminates data copy.
 * 
 * @author Nathan Tippy
 *
 */
public class TextDelegate implements CharSequence {

	final TextHeap heap;
	final char[] optionalConstant;
	int choice;
	
	TextDelegate(TextHeap heap, char[] constant) {
		this.heap = heap;
		this.optionalConstant = constant;
	}
	
	void setValue(int choice) {
		this.choice = choice;
	}
	
	public boolean isConstant() {
		return choice<0;
	}
	
	public boolean isOriginal() {
		return choice==0;
	}
	
	public boolean equals(CharSequence value) {
		if (choice<0) {
			//USE THE CONSTANT VALUE
			int len = optionalConstant.length;
			if (len!=value.length()) {
				return false;
			}
			int i = 0;
			while (i<len) {
				if (value.charAt(i)!=optionalConstant[i++]) {
					return false;
				}
			}
			return true;
		} else {
			return heap.equals(choice, value);
		}
	}
	
	public boolean equals(char[] value, int valueIdx, int valueLength) {
		if (choice<0) {
			//USE THE CONSTANT VALUE
			int len = optionalConstant.length;
			if (len!=valueLength) {
				return false;
			}
			int i = 0;
			while (i<len) {
				if (value[valueIdx+i]!=optionalConstant[i++]) {
					return false;
				}
			}
			return true;
		} else {
			return heap.equals(choice,value, valueIdx, valueLength);
		}
	}
	
	
	public void copyTo(Appendable target) {
		if (choice<0) {
			//USE THE CONSTANT VALUE
			int len = optionalConstant.length;
			int i = 0;
			while (i<len) {
				try {
					target.append(optionalConstant[i++]);
				} catch (IOException e) {
					throw new FASTException(e);
				}
			}
		} else {
			heap.get(choice,target);
		}
	}
	
	public int copyTo(char[] target, int targetIdx) {
		if (choice<0) {
			//USE THE CONSTANT VALUE
			int len = optionalConstant.length;
			int i = 0;
			while (i<len) {
				target[targetIdx+i]=optionalConstant[i++];
			}
			return optionalConstant.length;
		} else {
			return heap.get(choice,target,targetIdx);
		}
	}

	@Override
	public int length() {
		if (choice<0) {
			return optionalConstant.length;
		} else {
			return heap.length(choice);
		}
	}

	@Override
	public char charAt(int index) {
		if (choice<0) {
			return optionalConstant[index];
		} else {
			return heap.getChar(choice,index);
		}
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		//One of the few methods that will create a new object, use with care!
		//
		if (choice<0) {
			return new String(optionalConstant,start,end);
		} else {
			return heap.getSub(choice,start,end);
		}
	}
	
	
}
