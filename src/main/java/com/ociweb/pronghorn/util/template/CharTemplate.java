package com.ociweb.pronghorn.util.template;

import java.io.IOException;

public class CharTemplate<T> {

	CharTemplateData<T>[] script;
	int count;
	
	public CharTemplate() {
		script = new CharTemplateData[8];
	}
	
	private void append(CharTemplateData<T> fetchData) {
		
		if (count==script.length) {			
			CharTemplateData<T>[] newScript = new CharTemplateData[script.length*2];
			System.arraycopy(script, 0, newScript, 0, script.length);
			script = newScript;
		}
		script[count++] = fetchData;
		
	}
	
	
	public CharTemplate<T> add(final String text) {
	
		append(
					new CharTemplateData<T>() {
						@Override
						public void fetch(Appendable target, T source) {
							
							try {
								target.append(text);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
							
						}					
					}
				);
		
		return this;
		
	}
	
    public CharTemplate<T> add(CharTemplateData<T> data) {		
    	append(data);    	
		return this;
	}
	
    public void render(Appendable target, T source) {
   
    	for(int i=0;i<count;i++) {
    		script[i].fetch(target, source);
    	}
    	
    }
	
}
