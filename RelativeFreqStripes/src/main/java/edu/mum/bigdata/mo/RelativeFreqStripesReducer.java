package edu.mum.bigdata.mo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RelativeFreqStripesReducer
		extends
		Reducer<Text, MapWritable, TextPair, FloatWritable> {

	@Override
	protected void setup(
			Reducer<Text, MapWritable, TextPair, FloatWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		System.out.println("Reducer Output:");
	}

	@Override
	protected void reduce(Text key,
			Iterable<MapWritable> values, 
			Context context)
			throws IOException, InterruptedException {
		List<MapWritable> cache = new ArrayList<MapWritable>();

		CustomHashMap agg = new CustomHashMap();
		int count = 0;
		for (MapWritable hm : values) {
			cache.add(hm);
			for(Writable k:hm.keySet()){
				Text t = (Text) k;
				count += ((IntWritable) hm.get(k)).get();
				if(agg.containsKey(t)){
					int old = agg.get(t).get();
					old += ((IntWritable) hm.get(k)).get();
					agg.put(t, new IntWritable(old));
				} else {
					agg.put(t, new IntWritable(((IntWritable) hm.get(k)).get()));
				}
			}
		}
		
		for(Text k2:agg.keySet()){
			int sum = agg.get(k2).get();

			float relFreq = (1.f * sum) / (1.f * count);

			DecimalFormat df = new DecimalFormat("#.00");
			relFreq = Float.parseFloat(df.format(relFreq));
			TextPair tp = new TextPair(key, k2);
			System.out.println(new GenericPair<TextPair, Float>(tp,relFreq));
			context.write(tp, new FloatWritable(relFreq));
		}
		/*for(CustomHashMap hm:cache){
			for (Text k2 : hm.keySet()) {
				int sum = hm.get(k2).get();

				float relFreq = (1.f * sum) / (1.f * count);

				DecimalFormat df = new DecimalFormat("#.00");
				relFreq = Float.parseFloat(df.format(relFreq));
				TextPair tp = new TextPair(key, k2);
				System.out.println(new GenericPair<TextPair, Float>(tp,relFreq));
				context.write(tp, new FloatWritable(relFreq));
			}

		}*/
		agg = null;

	}

}