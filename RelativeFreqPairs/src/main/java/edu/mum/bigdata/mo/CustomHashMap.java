package edu.mum.bigdata.mo;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

/**
 * Created by Mo nuaimat on 9/11/16.
 */
public class CustomHashMap <K, V> extends HashMap<K, V> {
    @Override
    public String toString(){
        String ret = "{";
        ArrayList<String> elements = new ArrayList<>();
        for(K k:this.keySet()){
            elements.add(k.toString()+"="+get(k).toString());
        }
        
        ret += StringUtils.join(elements, ", ");
        ret += "}";
        
        return ret;
    }
}
