package edu.mum.bigdata.mo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFreqStripesMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		ArrayList<Text> words = new ArrayList<>();
		while (tokenizer.hasMoreTokens()) {
			words.add(new Text(tokenizer.nextToken()));
		}

		ArrayList<GenericPair<Text, MapWritable>> neighboursList = findNeighbourHashMap(
				words.remove(0), words);

		System.out.println("Mapper Output:");
		for (GenericPair<Text, MapWritable> gp : neighboursList) {	
			System.out.println(gp);
			context.write(gp.getKey(), gp.getVal());
		}
	}

	private static ArrayList<GenericPair<Text, MapWritable>> findNeighbourHashMap(
			Text w, ArrayList<Text> list) {
		
		if (list.size() == 1) {
			MapWritable hm = new MapWritable();
			hm.put(list.remove(0), new IntWritable(1));
			GenericPair<Text, MapWritable> gp = new GenericPair<>(w, hm);
			return new ArrayList<>(Arrays.asList(gp));
		}

		ArrayList<GenericPair<Text, MapWritable>> ret = new ArrayList<>();
		
		HashMap<String, Integer> hm = new HashMap<>();
		for (Text w2 : list) {
			if (w2.equals(w)) {
				break;
			}
			if(hm.containsKey(w2.toString())){
				int oldValue = hm.get(w2.toString());
				hm.put(w2.toString(), oldValue + 1);
			} else {
				hm.put(w2.toString(), 1);
			}
			
		}
		
		MapWritable mw = new MapWritable();
		for(String s:hm.keySet()){
			mw.put(new Text(s), new IntWritable(hm.get(s)));
		}
		
		ret.add(new GenericPair<Text, MapWritable>(w, mw));
		ret.addAll(findNeighbourHashMap(list.remove(0), list));
		return ret;
	}
}