import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class GraphMap extends Mapper<Object, Text, Text, Text> {

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        //1;"2,5|GRIS|0"
        String[] test=value.toString().split(";");
        IntWritable noeudId = new IntWritable(Integer.valueOf(test[0]));
        String[] test2 = test[1].toString().split("|");

        String [] voisins = test2[0].toString().split(",");

        String couleur = test2[1].toString();
        String couleurVois = "BLANC";

        IntWritable  pronfondeur = new IntWritable(Integer.valueOf(test2[2]));
         String[] values = null;

        if (couleur=="GRIS"){
            for (int i=0 ;i<=voisins.length;i++){
                couleurVois ="GRIS";
                IntWritable filsId= new IntWritable(Integer.valueOf(voisins[i]));
                values[i] = filsId+";"+couleurVois+"|"+pronfondeur+1;
            }
            couleur="NOIR";
        }
        else
            values[0] = noeudId+";"+couleur+"|"+pronfondeur;;

    }}
