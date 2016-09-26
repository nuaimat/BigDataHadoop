package edu.mum.bigdata.mo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RelFreqHybridReducer extends Reducer<TextPair, IntWritable, TextPair, FloatWritable> {

	private static TreeMap<String, Integer> buffer = new TreeMap<>();
	private static String lastKey = null;
	
	@Override
	protected void reduce(
			TextPair key,
			Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		String thisKey = key.getKey().toString();
		String thisVal = key.getVal().toString();
		
		if(!thisKey.equals(lastKey)){
			flushBuffer(lastKey, context);
			lastKey = new String(thisKey);
		}
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()){
		IntWritable i = it.next();
			if(!buffer.containsKey(thisVal)){
				buffer.put(thisVal, i.get());
			} else {
				Integer oldValue  = buffer.get(thisVal);
				buffer.put(thisVal, new Integer(oldValue +  i.get()));
			}
		} 
	}

	private void flushBuffer(
			String thisKey,
			Context context) throws IOException, InterruptedException {
		
		int hmSum = 0;
		for(String k:buffer.keySet()){
			hmSum += buffer.get(k);
		}
		
		for(String k:buffer.keySet()){
			float val = 1.f * buffer.get(k);
			float relFreq = val / (hmSum*1.f);
			DecimalFormat df = new DecimalFormat("#.00"); 
			relFreq = Float.parseFloat(df.format(relFreq));
			TextPair gp = new TextPair(new Text(thisKey), new Text(k));
			context.write(gp, new FloatWritable(relFreq) );
		}
		
		buffer = new TreeMap<>();
	}

	@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		System.out.println("cleaning up! lastKey: " + lastKey);
		flushBuffer(lastKey, context);
		lastKey =  null;
		buffer = new TreeMap<>();
	}
	
	
	
 }