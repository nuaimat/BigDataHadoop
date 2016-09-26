package edu.mum.bigdata.mo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	GenericPair<Text, Text> pair = new GenericPair<>();
	public TextPair(){
		this(new Text(), new Text());
	}
	public TextPair(Text a,Text b){
		pair.setKey(a);
		pair.setVal(b);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		this.pair.getKey().write(out);
		this.pair.getVal().write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.pair.key.readFields(in);
		this.pair.val.readFields(in);
	}
	
	@Override
	public int compareTo(TextPair o) {
		return this.pair.compareTo(o.pair);
	}
	public Text getKey() {
		return this.pair.getKey();
	}
	public Text getVal(){
		return this.pair.getVal();
	}
	
	@Override
	public String toString(){
		return this.pair.toString();
	}
		
	
}
