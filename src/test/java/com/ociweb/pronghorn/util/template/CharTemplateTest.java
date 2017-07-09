package com.ociweb.pronghorn.util.template;

import org.junit.Test;

import com.ociweb.pronghorn.util.Appendables;

public class CharTemplateTest {

	int testCounter;

	//NOTE: this is much better with lambdas in 8 but we must build 7 for android
	CharTemplateData<CharTemplateTest> injectValue = 
			new CharTemplateData<CharTemplateTest>() {
		@Override
		public void fetch(Appendable target,
				CharTemplateTest source) {
			Appendables.appendValue(target, source.testCounter);
		}
	};
	
	@Test
	public void testCharTemplate() {
		

				
		CharTemplate<CharTemplateTest> template = 
				         new CharTemplate<CharTemplateTest>()
				                .add("root/")
				                .add(injectValue)
				                .add("/tail");

		//the builder is kept and held for the long term
		StringBuilder builder = new StringBuilder();
				
		int i = 10;
		while(--i>=0) {
			testCounter=i;
			builder.setLength(0);
			template.render(builder, this);
			//System.out.println(builder);
		}
		
		
	}
	
	
}
