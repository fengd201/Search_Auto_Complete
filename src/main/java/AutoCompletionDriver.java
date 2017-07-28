
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class AutoCompletionDriver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        String inputDir = args[0];
        String nGramLibDir = args[1];
        String numberOfNGram = args[2];

        // job1 - build N-Gram library
        Configuration conf1 = new Configuration();
        conf1.set("noGram", numberOfNGram);
        conf1.set("textinputformat.record.delimiter", ".");

        Job job1 = Job.getInstance(conf1);
        job1.setJobName("BuildNGramLibJob");
        job1.setJarByClass(AutoCompletionDriver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLibDir));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
