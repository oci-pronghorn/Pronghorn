package com.ociweb.pronghorn.util;

import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectedKeyHashMapHolder {

    private boolean checkedForKeyMap;
    private HashMap<SelectionKey,?> keyMap;
    private final static Logger logger = LoggerFactory.getLogger(SelectedKeyHashMapHolder.class);
	
    public HashMap<SelectionKey,?> selectedKeyMap(Set<SelectionKey> selectedKeys) {
		if (!checkedForKeyMap && null==keyMap) {
        	   checkedForKeyMap = true;
	           Field[] fields = selectedKeys.getClass().getDeclaredFields();
	           int f = fields.length;
	           while (--f>=0) {
	        	   fields[f].setAccessible(true);
	        	   Field[] fields2;
				   try {
						Object objectF = fields[f].get(selectedKeys);
						if (objectF instanceof HashSet) {
							fields2 = objectF.getClass().getDeclaredFields();
							int f2 = fields2.length;
							while (--f2>=0) {
								 fields2[f2].setAccessible(true);
								 Object map = fields2[f2].get(objectF);						 
								 if (map instanceof HashMap) {
									 HashMap<SelectionKey, ?> localMap = (HashMap<SelectionKey,?>)map;
									 
									 SelectionKeyHashMap lhm = new SelectionKeyHashMap(32);
									 lhm.putAll(localMap);
								
									 fields2[f2].set(objectF, lhm);
									 keyMap = lhm;
								}		
								 fields2[f2].setAccessible(false); 
							}
						}					
					} catch (Throwable e) {
						//ignore we will fall back
						logger.info("unable to find map",e);
					}
					fields[f].setAccessible(false);
	           }
        }
		return keyMap;		
	}
	
}
