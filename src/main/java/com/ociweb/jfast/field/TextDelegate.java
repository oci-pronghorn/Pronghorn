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

	final int idx;
	final TextHeap heap;
	final char[] optionalConstant;
	int choice;
	
	TextDelegate(int idx, TextHeap heap, char[] constant) {
		this.idx = idx;
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
			if (choice>0) {
				//USE A SELECTED TEXT FROM THE HEAP
				return heap.equals(choice-1,value);
			} else {
				//USE THE FINAL TEXT INDEX
				return heap.equals(idx, value);
			}
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
			if (choice>0) {
				//USE A SELECTED TEXT FROM THE HEAP
				return heap.equals(choice-1,value, valueIdx, valueLength);
			} else {
				//USE THE FINAL TEXT INDEX
				return heap.equals(idx, value, valueIdx, valueLength);
			}
		}
	}
	
	
	public void get(Appendable target) {
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
			if (choice>0) {
				//USE A SELECTED TEXT FROM THE HEAP
				heap.get(choice-1,target);
			} else {
				//USE THE FINAL TEXT INDEX
				heap.get(idx, target);
			}
		}
	}
	
	public int get(char[] target, int targetIdx) {
		if (choice<0) {
			//USE THE CONSTANT VALUE
			int len = optionalConstant.length;
			int i = 0;
			while (i<len) {
				target[targetIdx+i]=optionalConstant[i++];
			}
			return optionalConstant.length;
		} else {
			if (choice>0) {
				//USE A SELECTED TEXT FROM THE HEAP
				return heap.get(choice-1,target,targetIdx);
			} else {
				//USE THE FINAL TEXT INDEX
				return heap.get(idx, target,targetIdx);
			}
		}
	}

	@Override
	public int length() {
		if (choice<0) {
			return optionalConstant.length;
		} else {
			if (choice>0) {
				return heap.length(choice-1);
			} else {
				return heap.length(idx);
			}
		}
	}

	@Override
	public char charAt(int index) {
		if (choice<0) {
			return optionalConstant[index];
		} else {
			if (choice>0) {
				return heap.getChar(choice-1,index);
			} else {
				return heap.getChar(idx, index);
			}
		}
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		//One of the few methods that will create a new object, use with care!
		//
		if (choice<0) {
			
			return new String(optionalConstant,start,end);

		} else {
			char[] sub = new char[end-start];
			if (choice>0) {
				
		//TODO:		heap.getSub(choice-1,sub,start,end);
			} else {
		//TODO:		heap.getSub(idx,sub,start,end);
			}
			return new String(sub);
		}
	}
	
	
}
