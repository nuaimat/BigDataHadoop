package edu.mum.bigdata.mo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class RelativeFreqPairsReducer extends Reducer<TextPair, IntWritable, TextPair, FloatWritable> {
	@Override
	protected void setup(
			Reducer<TextPair, IntWritable, TextPair, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		System.out.println("Reducer Output:");
	}
	private HashMap<Text, Integer> termsFreqTotal = new HashMap<>();
	@Override
	protected void reduce(
			TextPair key,
			Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        Text right = key.getVal(); 
        Text left = key.getKey();
        if(right.toString().equals("*")){
        	termsFreqTotal.put(left, sum );
			return;
		} else {
			int count = termsFreqTotal.get(left);
			float relFreq = (1.f * sum) / (1.f * count);
			DecimalFormat df = new DecimalFormat("#.00"); 
			relFreq = Float.parseFloat(df.format(relFreq));
			System.out.println(new GenericPair<TextPair, Float>(key, relFreq));
			context.write(key, new FloatWritable(relFreq));
		}
        
	}
	
 }