package com.ociweb.pronghorn.util;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class PinningUtil {

	//new core afinity scheduler? for the ScriptedFixedThread...
	//switch all threads to 1 or 2 of the cores.
	//the remaining cores are each matched to a specific scheduler 
	//rebalance the work every minute or so based on time and load.
	//1, count cores, reduce threads to match, and assign.
	//if we have too many threads group them on cores anyway?
	
	
	//get the pid first
	//pgrep java
	//
	//thread dump for this process
	//jstack -l 9291
    //
	//find the name of the thread we want to pin.
	//grep main <stack dump> | grep nid
	//
	//get the nid and convert from hex to decimal
	//awk -F'nid=| runnable' '{printf "%d",$2}'
	//
	///all together
	//jstack -l `pgrep java` | grep main | grep nid | awk -F'nid=| runnable' '{printf "%d",$2}'
    //take value
	
	//pin the thread to this core
	//taskset -p 0x00000001 <java thread pid>
	//read afinity
	//taskset -p <java thread pid>
	//NOTE: for general use FF is used for all cores.
	
	///perf stat
	//nate@Noah:~$ pgrep java
	//9291
	//nate@Noah:~$ perf stat -p 9291

	public static boolean visitStacks() {
		
		AppThreadVisitor visitor = new AppThreadVisitor() {

			@Override
			public void visit(long threadId, long threadTId, long threadNId) {
				System.err.println("found thread "+threadId+" with id "+threadNId);
			}

		};
		
		
		try {
			TrieParserReader reader = new TrieParserReader(10,true);
			Process process = Runtime.getRuntime().exec("jstack -l "+getPid());
		    InputStream stream = process.getInputStream();	
		    
		    byte[] buffer = new byte[1<<17]; //128K //must be power of 2
		    int bufferPos = 0;
		    
		    int val;
		    while((val = stream.read())>=0) {
		    	
		    	if (bufferPos == buffer.length) {
		    		//grow
		    		byte[] temp = new byte[bufferPos*2];
		    		System.arraycopy(buffer, 0, temp, 0, buffer.length);
		    		buffer = temp;
		    	}
		    	buffer[bufferPos++] = (byte)val;		 		    	
		    }
		    		    
		    TrieParserReader.parseSetup(reader, buffer, 0, bufferPos, Integer.MAX_VALUE);
		    		    
		    TrieParser parser = jstackParser();
		    
		    while (TrieParserReader.parseHasContent(reader)) {
		    	long id = reader.parseNext(parser);
		    	//System.err.println("parsed "+id);
		    	if (id==400) {
		    		System.err.println("unknown line found:");
		    		TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, System.err);
		    		return false;
		    	}
		    	if (id==-1) {
		    		System.err.println("unknown line found:");
		    		TrieParserReader.debugAsUTF8(reader, System.err);
		    		return false;
		    	}
		    	
		    	if (id<=2) {
		    		System.err.println("visiting thread:");
		    		reader.capturedFieldBytesAsUTF8(reader, 0, System.err);
		    		System.err.println();
		    	}
		    	
		    	visitAppThreads(id,reader,visitor);
		    }
		    
		} catch (Throwable e) {
			return false;
		}		
		
		return true;
	}

	private static final int[] jstackIdOffset   = new int[]{-1,1,1};
	private static final int[] jstackTIDOffset   = new int[]{2,4,5};
	private static final int[] jstackNIDOffset   = new int[]{3,5,6};
	
	private static void visitAppThreads(long id, TrieParserReader reader, AppThreadVisitor visitor) {
		if (id==1 || id==2) {
			
			long threadId  = reader.capturedLongField(reader, jstackIdOffset[(int)id]);
			long threadTId = reader.capturedLongField(reader, jstackTIDOffset[(int)id]);
			long threadNId = reader.capturedLongField(reader, jstackNIDOffset[(int)id]);
					
			visitor.visit(threadId,threadTId,threadNId);
			
		}		
		
	}
	
	
	
	private static TrieParser jstackParser;
	private static TrieParser jstackParser() {
		if (null==jstackParser) {
			TrieParser parser = new TrieParser();
	
			parser.setUTF8Value("\"%b\" os_prio=%i tid=%i nid=%i %b\n",               0);
			parser.setUTF8Value("\"%b\" #%i prio=%i os_prio=%i tid=%i nid=%i %b\n",   1);
			parser.setUTF8Value("\"%b\" #%i %b prio=%i os_prio=%i tid=%i nid=%i %b\n",2);
							
			parser.setUTF8Value("%i-%i-%i %i:%i:%i\nFull thread dump %b:\n",6);
			parser.setUTF8Value("   java.lang.Thread.State:%b\n",7);
			parser.setUTF8Value("	at %b\n",8);
					
			parser.setUTF8Value("   Locked ownable synchronizers:%b\n",9);
			parser.setUTF8Value("	-%b\n",10);
			parser.setUTF8Value("   No compile task\n",11);
			parser.setUTF8Value("   Compiling: %i %b\n",11);
			
			parser.setUTF8Value("\n",12);
			
			parser.setUTF8Value("JNI global references: %i\n",14);
			parser.setUTF8Value("%b\n",400);//unknown row
			jstackParser = parser;
		}
		
		return jstackParser;
	}
	
	
	public static <A extends Appendable> boolean getJStack(A target) {
		
		visitStacks();
		
		try {
			Process process = Runtime.getRuntime().exec("jstack -l "+getPid());
		    InputStream stream = process.getInputStream();		    
		    byte[] backing = stream.readAllBytes();
		    Appendables.appendUTF8(target, backing, 0, backing.length, Integer.MAX_VALUE);
		  
		} catch (Throwable e) {
			return false;
		}		
		
		return true;
	}
	
	
	public static Integer getPid() {
		RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
		Field jvm;
		try {
			jvm = runtime.getClass().getDeclaredField("jvm");
			jvm.setAccessible(true);
			Object mgtObj = jvm.get(runtime);
			Method pid_method = mgtObj.getClass().getDeclaredMethod("getProcessId");
			pid_method.setAccessible(true);
			
			Integer result = (Integer) pid_method.invoke(mgtObj);
			pid_method.setAccessible(false);
			jvm.setAccessible(false);
			
			return result;
		} catch (Throwable t) {
			//unable to process on this platform or version
			return null;
		}
	}
	
	
	
	
}
