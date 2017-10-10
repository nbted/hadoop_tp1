import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        String[] ourArgs=new GenericOptionsParser(configuration, args).getRemainingArgs();
        Job job = new Job.getInstance(configuration,"graphe parcours");


        job.setJarByClass(Driver.class);
        job.setMapperClass(GraphMap.class);
        job.setReducerClass(GraphReduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        while(!job.isComplete()) {
            FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));
        }

        if (job.waitForCompletion(true)){
            System.exit(0);
        }
        System.exit(-1);
}
}
