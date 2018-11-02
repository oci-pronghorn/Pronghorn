package com.ociweb.pronghorn.util.template;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.ociweb.pronghorn.util.AppendableBuilder;
import com.ociweb.pronghorn.util.Appendables;

public class StringTemplateBuilderTest {

	
	class DemoObject {

		private final int id;
		private String message;
		
		public DemoObject(int id, String message) {
			this.id = id;
			this.message = message;
		}
		
		public int getId() {
			return id;			
		}

		public String getMessage() {
			return message;
		}
		
	}
/*	
	<!DOCTYPE html> <html> <head><title>Fortunes</title></head> <body> <table> <tr><th>id</th><th>message</th></tr>
	 {{#.}} <tr><td>{{id}}</td><td>{{message}}</td></tr> {{/.}}
    </table> </body> </html>
	
	
	<script>alert("This should not be displayed in a browser alert box.")</script>
	
	&lt;script&gt;alert(&quot;This should not be displayed in a browser alert box.&quot;);&lt;/script&gt;
	
*/
	
	@Test
	public void buildHTMLTest() {

		StringTemplateRenderer<List<DemoObject>> template =		
									new StringTemplateBuilder<List<DemoObject>>()
									       .add("<!DOCTYPE html> <html> <head><title>Fortunes</title></head> <body> <table> <tr><th>id</th><th>message</th></tr>\n")
									       .add((t,s,i)-> {
												if (i<s.size()) {													
													Appendables.appendHTMLEntityEscaped(
														Appendables.appendValue(t, 
																"<tr><td>", s.get(i).getId(),"</td><td>"), s.get(i).getMessage() ).append("</td></tr>\n");
													return true;
												} else {
													return false;
												}
									         })		
									       .add("</table></body></html>")
									       .finish();
	
		////////////////////
		//build example data
		////////////////////
		
		List<DemoObject> obj = new ArrayList<DemoObject>();
		obj.add(new DemoObject(1,"<script>alert(\"This should not be displayed in a browser alert box.\")</script>"));
		obj.add(new DemoObject(2,"A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1"));
		obj.add(new DemoObject(3,"フレームワークのベンチマーク"));
			
		///////////////////
		//apply the template
		////////////////////
		AppendableBuilder target = new AppendableBuilder(1<<21);
		template.render(target, obj);
		String result = target.toString();
		///////////////////
		
		String expected = "<!DOCTYPE html> <html> <head><title>Fortunes</title></head> <body> <table> <tr><th>id</th><th>message</th></tr>\n" + 
				"<tr><td>1</td><td>&lt;script&gt;alert(&quot;This should not be displayed in a browser alert box.&quot;)&lt;/script&gt;</td></tr>\n" + 
				"<tr><td>2</td><td>A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1</td></tr>\n" + 
				"<tr><td>3</td><td>フレームワークのベンチマーク</td></tr>\n" + 
				"</table></body></html>";
				
		assertEquals(expected, result);	
		
	}
	
	
}
