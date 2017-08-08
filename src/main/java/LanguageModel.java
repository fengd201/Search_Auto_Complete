import dto.DBOutputWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import to.WordsCountTO;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class LanguageModel {
    public static class LangModelMapper extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;
        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("threshold", 5);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input value: This is an example\t5
            // output key: This is an
            // output value: example=5
            String line = value.toString().trim();
            String[] wordsCount = line.split("\t");
            if (wordsCount.length < 2) {
                // TODO throw bad input exception
                return;
            }

            // remove useless data based on threshold
            int count = Integer.parseInt(wordsCount[1]);
            if (count < threshold) {
                return;
            }

            StringBuilder sb = new StringBuilder();
            String[] words = wordsCount[0].split("\\s+");
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i]).append(" ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length-1] + "=" + wordsCount[1];
            context.write(new Text(outputKey), new Text(outputValue));
        }

    }

    public static class LangModelReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int topK;
        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            topK = configuration.getInt("topK",2);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input key: This is an
            // input value: <example=5, apple=8, orange=9, defect=100>
            // output key: This is an|defect|100
            // output value: null
            PriorityQueue<WordsCountTO> wordsCountQueue = new PriorityQueue<WordsCountTO>(new Comparator<WordsCountTO>() {
                // reverse order
                public int compare(WordsCountTO o1, WordsCountTO o2) {
                    return -o1.compareTo(o2);
                }
            });

            for (Text value : values) {
                int count = Integer.parseInt(value.toString().split("=")[1]);
                String word = value.toString().split("=")[0];
                wordsCountQueue.offer(new WordsCountTO(count, word));
            }
            WordsCountTO wordsCountTO = new WordsCountTO(0,"");
            for (int i = 0; i < topK; i++) {
                if (wordsCountQueue.isEmpty()) {
                    break;
                }
                wordsCountTO = wordsCountQueue.poll();
            }
            context.write(new DBOutputWritable(key.toString(), wordsCountTO.getWord(), wordsCountTO.getCount()), NullWritable.get());
        }
    }
}
