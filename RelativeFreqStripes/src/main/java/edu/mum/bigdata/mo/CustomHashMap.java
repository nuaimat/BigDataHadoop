package edu.mum.bigdata.mo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by Mo nuaimat on 9/11/16.
 */
public class CustomHashMap extends HashMap<Text, IntWritable> implements WritableComparable<CustomHashMap> {
    @Override
    public String toString(){
        String ret = "{";
        ArrayList<String> elements = new ArrayList<>();
        for(Text k:this.keySet()){
            elements.add(k.toString()+"="+get(k).toString());
        }
        
        ret += StringUtils.join(elements, ", ");
        ret += "}";
        
        return ret;
    }

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		for(Text t:this.keySet()){
			TextPair tp = new TextPair(t, new Text("" + get(t)) );
			tp.write(out);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for(int i=0;i<size;i++){
			TextPair tp = new TextPair();
			tp.readFields(in);
			this.put(tp.getKey(), new IntWritable(Integer.parseInt(tp.getVal().toString())));
		}
		
	}

	@Override
	public int compareTo(CustomHashMap o) {
		Text t1 = new Text();
		Text t2 = new Text();
		for(Text t:this.keySet()){
			t1 = t;
			break;
		}
		
		for(Text tt:o.keySet()){
			t1 = tt;
			break;
		}
		
		return t1.compareTo(t2);
	}
}
