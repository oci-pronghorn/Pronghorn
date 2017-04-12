package com.ociweb.pronghorn.util;

import java.io.IOException;

public class TopicUtil {

	//TODO: (easy short task) add unit tests for this.
	
	public static long extractLong(CharSequence topic, int levelIndex) {
		
		long result = 0;
		
		int limit = topic.length();
		int idx = scanForLevelPosition(topic, levelIndex, 0, limit);
		
		while (idx<limit) {
			char c = topic.charAt(idx);
			
			if (c>='0' && c<='9') {							
				result = ((result*10) + (c-'0'));				
			} else {
				//not numeric found, probably /
				break;
			}
			
		}
		return result;
	}
	
	public static CharSequence extractCharSequence(CharSequence topic, int levelIndex) {
		
		long result = 0;
		
		int limit = topic.length();
		int idx = scanForLevelPosition(topic, levelIndex, 0, limit);
		
		int start = idx;
		int length = 0;
		while (idx<limit && topic.charAt(idx)!='/') {
			length++;
		}
		return topic.subSequence(start, start+length);
	}
	
	public static void extractCharSequence(CharSequence topic, int levelIndex, Appendable target) {
		
		long result = 0;
		
		int limit = topic.length();
		int idx = scanForLevelPosition(topic, levelIndex, 0, limit);
		
		int start = idx;
		int length = 0;
		while (idx<limit && topic.charAt(idx)!='/') {
			length++;
		}
		
		try {
			target.append(topic, start, start+length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}	
	

	private static int scanForLevelPosition(CharSequence topic, int levelIndex, int idx, int limit) {
		int levelsRemaining = levelIndex;
		while (levelsRemaining>0 && idx<limit) {
			if (topic.charAt(idx) == '/') {
				levelsRemaining--;
			}
			idx++;
		}
		return idx;
	}

}
