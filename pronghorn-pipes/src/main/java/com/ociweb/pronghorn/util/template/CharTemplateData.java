package com.ociweb.pronghorn.util.template;

public interface CharTemplateData<T> {

	void fetch(Appendable target, T source);

}
