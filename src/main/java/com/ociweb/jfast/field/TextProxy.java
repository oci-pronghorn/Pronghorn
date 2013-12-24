package com.ociweb.jfast.field;

public class TextProxy {

	final int idx;
	final TextHeap heap;
	
	TextProxy(int idx, TextHeap heap) {
		this.idx = idx;
		this.heap = heap;
	}
	
	public void get(Appendable target) {
		heap.get(idx, target);
	}
	
	public void get(char[] target, int targetIdx) {
		heap.get(idx, target, targetIdx);
	}
	
	
	
}
