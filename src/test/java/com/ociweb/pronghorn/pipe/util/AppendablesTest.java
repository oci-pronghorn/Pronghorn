package com.ociweb.pronghorn.pipe.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Base64;
import java.util.Random;

import org.junit.Test;

import com.ociweb.pronghorn.util.Appendables;




public class AppendablesTest {

    private static int TEST_SIZE = 1000000;

    
    @Test
    public void appendArray(){
    	
    	byte[] a = new byte[1000];
    	 Random r = new Random(101);
              r.nextBytes(a);
    	StringBuilder str = new StringBuilder();
    	 Appendables.appendArray(str, a);
    	
    	    	//length of this stringbuilder(with 1000 bytes), with formatting (parens around negatives, commas, etc), will simply always be 5571.
    	   assertEquals(5571,str.length());    	   
    	   //test other append arrays
    	   
    	   long [] b = new long[1000];
    	   for(int i = 0;i<1000;i++){
    		   b[i] = r.nextLong();
    	   }
    	   str = new StringBuilder();
    	   Appendables.appendArray(str, 'l',b,'r');
    	  
    	   assertEquals(str.length(),22447); //same as above.
    	   //checks to make sure l and r were appended correctly.
    	   assertTrue(str.toString().charAt(0)=='l' && str.toString().charAt(str.length()-1)=='r');
    	   
    	   
    	
    	   str = new StringBuilder();
    	   //offset of 1. will skip first val of -23.
    	   Appendables.appendArray(str, a,1,a.length-1,a.length);
    	//length of str will be 5538.
    	   System.out.println();
    	   assertEquals(str.length(),5538);
    	   str = new StringBuilder();
    	 //  A target, char left, int[] b, long offset, int mask, char right, int bLength
   
    	   
    	   
    	   
    	   
    	   str = new StringBuilder();
    	   Appendables.appendArray(str,'L', a,  'R');
    
    	   assertEquals(str.length(),5571);
    	 	 assertEquals(str.toString().charAt(0),'L');//testing left and right chars were appended properly
    	    	assertEquals(str.toString().charAt(str.length()-1),'R');
    	    	
    	    	
    	    	
    	    	str = new StringBuilder();
    	    	Object [] obj = new Object[1000];
    	    
    	    	
    	    	for(int i = 0;i<obj.length;i++){
    	    	obj[i] = r.nextInt();	 //should this***** work smoothly if it was (char)r.nextInt(127)??*
    	    	//show nathan output in that case.
    	    	}
    	    	
    	    	Appendables.appendArray(str,'[',obj,']');
    	    	
    	    	assertEquals(str.length(),11945); 
       	 	 assertEquals(str.toString().charAt(0),'[');//testing left and right chars were appended properly
       	    	assertEquals(str.toString().charAt(str.length()-1),']');
       	    	
    
  //public static <A extends Appendable> A appendArray(A target, char left, int[] b, long offset, int mask, char right, int bLength) {
     	   int[] intarr = new int[10000];
    	   str = new StringBuilder();
    	   for(int i = 0;i<10000;i++)
    		   intarr[i] = r.nextInt();
    	    Appendables.appendArray(str,'[',intarr,0L,intarr.length-1,']',intarr.length);
    	    assertEquals(str.length(),130832); 
    	    assertEquals(str.toString().charAt(0),'[');//testing left and right chars were appended properly
   	    	assertEquals(str.toString().charAt(str.length()-1),']');
   	    	

       	    	
    }
    @Test
    public void appendValue(){
    	StringBuilder str = new StringBuilder();
    	Appendables.appendValue(str, "Label: ", 5, " -Suffix");
    	assertEquals(str.toString(),"Label: 5 -Suffix");
    	str = new StringBuilder();
    	Appendables.appendValue(str, "Label: ", 5);
    	
    	//System.out.println(str.toString());
    	assertEquals(str.toString(),"Label: 5");
    	
    	str = new StringBuilder();
    Appendables.appendValue(str, "Label:", Long.MAX_VALUE, "-Sufix");
  
    assertEquals(str.toString(),"Label:9223372036854775807-Sufix");
    
	str = new StringBuilder();
    Appendables.appendValue(str, "Label:", Long.MAX_VALUE);
  
    assertEquals(str.toString(),"Label:9223372036854775807");
    
    
    
  
    	
    }
    
    @Test
    public void appendFixedDecimalDigits(){
    	StringBuilder str = new StringBuilder();
    	Appendables.appendFixedDecimalDigits(str, -42,10 ); //ask nathan - if for example give 
    	//appendFixedDecimalDigits(str, -420,10 ) exhibits weird behavior. 
    	
    	assertEquals(str.toString(),"-42");
    	
       	str = new StringBuilder();
    	
    	long x = -100000L;
 
      	Appendables.appendFixedDecimalDigits(str, x ,100000000);
      	//is this a fair test?
      	assertEquals(str.toString(),"-000100000");
    	
    	
    }
    
    
    @Test
    public void appendHexArray(){
    	
    	int[] a = new int[1000];
    	Random rand = new Random(101);
    	  
    	byte [] b = new byte[1000];
    	rand.nextBytes(b);
    	
   	 for(int i = 0;i<a.length;i++){
   		 a[i] = rand.nextInt(0x10) + 0x10;  // Generates a random number between 0x10 and 0x20
   	 }
   	 
   	StringBuilder str = new StringBuilder();
   	 Appendables.appendHexArray(str, 'L', a, 0, 0xFF, 'R', a.length);
   	 
  
   	 assertEquals(str.toString().charAt(0),'L');//testing ledt and right chars were appended properly
   	assertEquals(str.toString().charAt(str.length()-1),'R');
   	assertEquals(str.length(),12000);//predictable length of 1000 int array with formatting char bytes.
	      
   	
   	//appendHexArray(A target, char left, byte[] b, int offset, int mask, char right, int bLength) {
	     
   	
   	 str = new StringBuilder();
   	Appendables.appendHexArray(str, 'L', b, 0, b.length-1, 'R', b.length);
   	assertEquals(str.toString().charAt(0),'L');//testing ledt and right chars were appended properly
   	assertEquals(str.toString().charAt(str.length()-1),'R');
   	assertEquals(str.length(),6000);//predictable length of 1000 byte array with formatting char bytes.
	      
   	
   	str = new StringBuilder();

  	 Appendables.appendArray(str, '[',a,']');
  	 
	 assertEquals(str.toString().charAt(0),'[');//testing ledt and right chars were appended properly
	 assertEquals(str.toString().charAt(str.length()-1),']');
	 assertEquals(str.length(),4000);//predictable length of 1000 int array with formatting char bytes.
		      
    }
    
    @Test
    public void appendedLength(){
    	
    	long val = Appendables.appendedLength(1000);
    	assertTrue(val==4);  
    }
    
    @Test
    public void wikipediaTest() {
    	String value = "Man is distinguished, not only by his reason, but by this singular passion from "
	    	+"other animals, which is a lust of the mind, that by a perseverance of delight "
	    	+"in the continued and indefatigable generation of knowledge, exceeds the short "
	    	+"vehemence of any carnal pleasure.";
    	
    	byte[] b = value.getBytes();
 
    	
    	StringBuilder str = new StringBuilder();
    	Appendables.appendBase64(str, b, 0, b.length, Integer.MAX_VALUE);
    	assertEquals(Base64.getEncoder().encodeToString(b), str.toString());
    	
//    	TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0aGlz
//    	IHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1c3Qgb2Yg
//    	dGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0aGUgY29udGlu
//    	dWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdlLCBleGNlZWRzIHRo
//    	ZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=

    	byte[] encBytes = str.toString().getBytes();
    	//from these bytes we must reassemble the original text
    	//each char must map to an integer.
    	
    	byte[] target = new byte[value.length()];
    	int len = Appendables.decodeBase64(encBytes, 0, encBytes.length, Integer.MAX_VALUE, 
    			                           target, 0, Integer.MAX_VALUE);
    	
    	assertEquals(value.length(), len);
    	assertEquals(value, new String(target,0,len));

    	
    }
    
    @Test
    public void appendBase64Encoded(){
    	StringBuilder str = new StringBuilder();
    	byte [] b = new byte[100];
    	Random r = new Random(101);
    	r.nextBytes(b);
    	String val1 = "6cDPuFQn8gA7NlFdPWxThG37yTwrIDXBxmeLjytbTdgNMSP9POqYoURhxkvhwRMm11q10IS2VdDFXDdxkwHqrZFnmb%2BB%2BKOyAYU3hQgDIJnsl4SeYWBDBK%2FOqDVYN7RL%2BekiCg%3D%3D";
    	String val2 = "6cDPuFQn8gA7NlFdPWxThG37yTwrIDXBxmeLjytbTdgNMSP9POqYoURhxkvhwRMm11q10IS2VdDFXDdxkwHqrZFnmb+B+KOyAYU3hQgDIJnsl4SeYWBDBK/OqDVYN7RL+ekiCg==";
    	Appendables.appendBase64Encoded(str, b, 0, b.length, Integer.MAX_VALUE);
    	assertEquals(val1, str.toString());
    	
    	
    	str = new StringBuilder();
    	Appendables.appendBase64(str, b, 0, b.length, Integer.MAX_VALUE);
     	assertEquals(val2, str.toString());

     	String x = Base64.getEncoder().encodeToString(b);
     	assertEquals(x, val2);
    	
     	
     	/////////////////
     	/////////////////
     	//convert back 
     	
     	byte[] data = Base64.getDecoder().decode(val2.getBytes());
     	assertArrayEquals(b, data);
	
     	
    	
    }
    
    @Test
    public void appendEpochTime(){
    	StringBuilder str = new StringBuilder();
    	Random r = new Random(101);
    	Appendables.appendEpochTime(str, 10000); //10 seconds
    
    	
    	assertEquals(str.toString(),"0:00:10.000");
  
    	 str = new StringBuilder();
    	 Appendables.appendEpochTime(str, 3600000); //1 hour effectively.
    	
   
    	 assertEquals(str.toString(),"1:00:00.000");
    }
    
    
    @Test
    public void appendIntDecimalValue() {
        
        Random r = new Random(101);
        
        for(int i = 0; i<TEST_SIZE; i++) {
            int value = r.nextInt();
            
                String actual = Appendables.appendValue(new StringBuilder(), value).toString();
                String expected = Integer.toString(value);
                if (value<0) {
                    expected = "("+expected+")";
                }
                assertEquals(expected, actual);
         
        }
        
    }

    @Test
    public void appendIntHexValue() {
        
        Random r = new Random(101);
        
        for(int i = 0; i<TEST_SIZE; i++) {
            int value = r.nextInt();
            
                String actual = Appendables.appendHexDigits(new StringBuilder(), value).toString().toLowerCase();
                String expected = "0x"+Integer.toHexString(value);
                assertEquals(""+i,expected, actual);
         
        }
    }  
    
    
    @Test
    public void appendIntHexFixedValue() {
        
        Random r = new Random(101);
        
        for(int i = 0; i<TEST_SIZE; i++) {
            int value = r.nextInt();
 
                String actual = Appendables.appendFixedHexDigits(new StringBuilder(), value, 32).toString().toLowerCase();
                String temp = "00000000"+Integer.toHexString(value);
                String expected = "0x"+temp.substring(temp.length()-8);
                assertEquals(""+i,expected, actual);                
                          
                
                //appendFixedHexDigits(A target, long value, int bits)
                StringBuilder str = new StringBuilder();
                Appendables.appendFixedHexDigits(str,10L, 8); //value 10 can be represented with 8 bits.
                          
                assertEquals(str.toString(),"0x0a");
                
                
                str = new StringBuilder();
                Appendables.appendHexDigits(str,100L);//0x64
                
                assertEquals(str.toString(),"0x64");
                
                
        }
    }  
    
    @Test
    public void appendSkipValue() {
        
        String originalText = "abcXYZdefXYyghiXklXYZ";
        String skipText = "XYZ";
        
        StringBuilder target = new StringBuilder();
        Appendables.appendAndSkip(target, originalText, skipText);

        
        String expected = originalText.replace(skipText,"");
        
        assertEquals(expected, target.toString());
                
    }
    
	@Test
	public void splitterTest() {
		
		String input = "/hello/this/is/text";
		
		CharSequence[] result = Appendables.split(input, '/');
				
		assertTrue("".equals(result[0]));
		assertTrue("hello".equals(result[1]));
		assertTrue("this".equals(result[2]));
		assertTrue("is".equals(result[3]));
		assertTrue("text".equals(result[4]));
		
	}
	
	
	@Test
	public void appendDecimalPostivieTest() {
		
		 long m = 12345;
		 
		 StringBuilder target = new StringBuilder();
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)2);
		 assertEquals("1234500",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)0);
		 assertEquals("12345",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)-2);
		 assertEquals("123.45",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)-6);
		 assertEquals("0.012345",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, 456, (byte)-6);
		 assertEquals("0.000456",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, 1, (byte)-6);
		 assertEquals("0.000001",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, 1, (byte)-3);
		 assertEquals("0.001",target.toString());
		 
	}
	
	@Test
	public void appendDecimalNegativeTest() {
		
		 long m = -12345;
		 
		 StringBuilder target = new StringBuilder();
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)2);
		 assertEquals("-1234500",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)0);
		 assertEquals("-12345",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)-2);
		 assertEquals("-123.45",target.toString());
		 
		 target.setLength(0);		 
		 Appendables.appendDecimalValue(target, m, (byte)-6);
		 assertEquals("-0.012345",target.toString());
		 		
	}
    
}
