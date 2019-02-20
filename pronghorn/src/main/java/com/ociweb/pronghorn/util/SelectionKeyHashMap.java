package com.ociweb.pronghorn.util;

import java.nio.channels.SelectionKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.ociweb.pronghorn.network.SelectionKeyHashMappable;

public class SelectionKeyHashMap extends HashMap<SelectionKey, Object> {

	//NOTE: this is built on identity equals.
	//NOTE: this is garbage free
	
	int count;
	int empty;
	SelectionKey[] keys;
	Object[] values;
		
	public SelectionKeyHashMap(int initSize) {
		keys = new SelectionKey[initSize];
		values = new Object[initSize];
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void clear() {
		int c = count;
		while (--c>=0) {
			if (null!=keys[c]) {
				((SelectionKeyHashMappable)keys[c].attachment()).skPosition(-1);
				keys[c] = null;
			}
		}
		count = 0;
	}

	@Override
	public Object put(SelectionKey k, Object v) {
		if (null==k.attachment() ) {
			k.attach(new SelectionKeyHashMappableImpl());
		}
		SelectionKey sk = (SelectionKey)k;
		int pos = ((SelectionKeyHashMappable)sk.attachment()).skPosition();
		if (pos>=0) {
			Object result = values[pos];
			values[pos] = v;
			return result;
		}
		
		//may need to grow
		if (count==keys.length) {
			SelectionKey[] newKeys  = new SelectionKey[count*2];
			Object[] newObjectalues = new Object[count*2];
			System.arraycopy(keys, 0, newKeys, 0, keys.length);
			System.arraycopy(values, 0, newObjectalues, 0, values.length);
			keys = newKeys;
			values = newObjectalues;			
		}
		keys[count] = k;
		values[count] = v;
		
		((SelectionKeyHashMappable)k.attachment()).skPosition(count);

		count++;
		return v;
	}
	
	@Override
	public boolean isEmpty() {
		return 0 == count;
	}


	@Override
	public void forEach(BiConsumer<? super SelectionKey, ? super Object> consumer) {
		int c = count;
		while (--c>=0) {
			if (null!=keys[c]) {
				consumer.accept(keys[c], values[c]);
			}
		}
	}

	@Override
	public Object remove(Object k) {
				
		SelectionKeyHashMappable selectionKeyHashMappable = (SelectionKeyHashMappable)((SelectionKey)k).attachment();
		int pos = selectionKeyHashMappable.skPosition();
		if (pos>=0) {
			keys[pos] = null;
			selectionKeyHashMappable.skPosition(-1);
			
			if (++empty == count) {
				count = 0;
				empty = 0;
			}
			return null;//should return old but not used.
		}
		//not found		
		return null;		
	}
	
	private final BiConsumer putter = new BiConsumer() {
		@Override
		public void accept(Object k, Object v) {
			put((SelectionKey)k,(Object)v);
		}		
	};

	@Override
	public void putAll(Map<? extends SelectionKey, ? extends Object> map) {
		map.forEach(putter);
	};

	@Override
	public boolean containsKey(Object k) {
		return ((SelectionKeyHashMappable)((SelectionKey)k).attachment()).skPosition()>=0;
	}	
	
	@Override
	public int size() {
		return count;
	}

	
	@Override
	public boolean containsValue(Object arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Entry<SelectionKey, Object>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object get(Object arg0) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<SelectionKey> keySet() {
		throw new UnsupportedOperationException();
	}


	@Override
	public Collection<Object> values() {
		throw new UnsupportedOperationException();
	}

}
