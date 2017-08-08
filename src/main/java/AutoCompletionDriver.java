
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class AutoCompletionDriver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        String inputDir = args[0];
        String nGramLibDir = args[1];
        String numberOfNGram = args[2];
        String threshold = args[3];
        String numberOfFollowingWords = args[4];

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
        job1.waitForCompletion(true);

        // job2
        Configuration conf2 = new Configuration();
        conf2.set("threshold", threshold);
        conf2.set("topK", numberOfFollowingWords);

        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:8889/test",
                "root",
                "root");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("LanguageModelJob");
        job2.setJarByClass(AutoCompletionDriver.class);

        //job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));
        job2.setMapperClass(LanguageModel.LangModelMapper.class);
        job2.setReducerClass(LanguageModel.LangModelReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.setInputPaths(job2, new Path(nGramLibDir));
        DBOutputFormat.setOutput(job2, "output", new String[] {"starting_phrase", "following_word", "count"});
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
