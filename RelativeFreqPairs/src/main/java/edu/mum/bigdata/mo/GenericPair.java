package edu.mum.bigdata.mo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by 985321 on 9/5/2016.
 */
public class GenericPair<S, T> implements WritableComparable {
    S key;
    T val;
    HashMap<String, String> meta = new HashMap<>();

    public GenericPair(){
    	this.key = (S) "";
    	this.val = (T) "";
    }
    
    public GenericPair(S s, T t) {
        this.key = s;
        this.val = t;
    }

    public T getVal() {
        return val;
    }

    public void setVal(T t) {
        this.val = t;
    }

    public S getKey() {
        return key;
    }

    public void setKey(S key) {
        this.key = key;
    }

    @Override
    public String toString(){
        if(this.val instanceof Integer){
            return String.format("< %s , %s >", this.key, (Integer)this.val==0?"*":this.val);
        }
        return String.format("< %s , %s >", this.key, this.val);
    }

 

    public void setMetaProperty(String s, String v){
        this.meta.put(s, v);
    }

    public String getMetaProperty(String s){
        return this.meta.get(s);
    }

    @Override
    public boolean equals(Object obj) {
        return this.key.equals( ((GenericPair)obj).getKey() ) && this.val.equals(  ((GenericPair)obj).getVal() );
    }

	@Override
	public void write(DataOutput out) throws IOException {
		String s = String.format("%s__--__%s", this.key, this.val);
		out.writeUTF(s);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String s = in.readUTF();
		String[] r = s.split("__--__");
		this.setKey((S) r[0]);
		this.setVal((T) r[1]);
		
	}

	@Override
	public int compareTo(Object o) {
		 if(! (o instanceof GenericPair) ) {
	            throw new RuntimeException("Can't compare to " + o);
	        }
	        GenericPair gp = (GenericPair) o;
	        if (this.getKey() instanceof  GenericPair){
	            return ((GenericPair) this.getKey()).compareTo( (GenericPair) gp.getKey() );
	        } else if (this.getKey() instanceof Comparable) {
	            int n = ((Comparable) this.key).compareTo((Comparable) gp.key);
	            if(n != 0){
	                return n;
	            }
	            return ((Comparable) this.val).compareTo((Comparable) gp.val);
	        }
	        
	        return 0;
	}

}
