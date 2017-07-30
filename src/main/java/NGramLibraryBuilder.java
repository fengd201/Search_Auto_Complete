import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;
        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            noGram = configuration.getInt("noGram", 5);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // read sentence by sentence
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
            // split by space
            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }
            // merge
            // from 2-Gram to n-Gram
            for (int i = 0; i < words.length - 1; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; j < noGram && i + j < words.length; j++) {
                    sb.append(" ").append(words[i+j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                    context.getCounter("Words", "word").increment(1);
                }
            }
        }

    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
