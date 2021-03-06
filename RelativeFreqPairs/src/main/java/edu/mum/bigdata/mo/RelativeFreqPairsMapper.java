package edu.mum.bigdata.mo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelativeFreqPairsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        ArrayList<Text> words = new ArrayList<>();
        while (tokenizer.hasMoreTokens()) {
        	words.add(new Text(tokenizer.nextToken()));
        }
        
        ArrayList<TextPair> neighboursList = 
        		findNeighbourPairs(words.remove(0), words);
        
        System.out.println("Mapper output: ");
        for(TextPair gp:neighboursList){
            TextPair counter = new TextPair(gp.getKey(), new Text("*"));
            
            context.write(gp, one);
            context.write(counter, one);
            
            System.out.println(new GenericPair<TextPair, Integer>(counter, 1));
            System.out.println(new GenericPair<TextPair, Integer>(gp, 1));
            
        }
    }
    
    
    public static ArrayList<TextPair> findNeighbourPairs(Text w, ArrayList<Text> list){
        if(list.size() == 1){
            TextPair gp = new TextPair( new Text(w) , new Text(list.remove(0)) );
            return new ArrayList<>(Arrays.asList(gp));
        }

        ArrayList<TextPair> ret = new ArrayList<>();
            for(Text w2:list){
                if(w2.equals(w)){
                    break;
                }
                ret.add(new TextPair(w, w2));
            }

        ret.addAll( findNeighbourPairs(list.remove(0), list) );
        return ret;
}
 } 