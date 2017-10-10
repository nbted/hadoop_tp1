
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.Iterator;

public class GraphReduce extends Reducer<Text, Text, Text, Text>{

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {

        Iterator i =values.iterator();

         String fils="" ;
        int prof =-1 ;
        String couleur ="BLANC";
        while(i.hasNext()){

                String [] val = i.toString().split("|");
                if (val[1]=="GRIS"){
                    couleur="GRIS";
                }
                if (Integer.valueOf(val[2]) > prof){
                    prof = Integer.valueOf(val[2]);
                }
                fils=val[0];

        }
        String noeud = key+";"+fils+"|"+couleur+""+prof;

    }
}

