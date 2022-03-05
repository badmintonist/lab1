package bdtc.lab1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Использование Combiner’а
 *
 *1  Используем  команду, чтобы создать каталог для хранения скомпилированных классов Java
 * $ mkdir units
 *
 * 2   Используем  команды для компиляции программы WordCount.java и для создания jar для программы.
 * javac -classpath hadoop-core-1.2.1.jar -d units WordCount.java
 * $ jar -cvf units.jar -C units/ .
 *
 * 3 Используем  команду для создания входного каталога в HDFS.
 * $HADOOP_HOME/bin/hadoop fs -mkdir input_dir
 *
 * 4 Используем  команду, чтобы скопировать входной файл с именем input.txt во входной каталог HDFS.
 * $HADOOP_HOME/bin/hadoop fs -put /home/hadoop/input.txt input_dir
 *
 *5  Используем команду, чтобы проверить файлы во входном каталоге.
 * $HADOOP_HOME/bin/hadoop fs -ls input_dir/
 *
 *6   Используем  команду, чтобы запустить приложение Word count, взяв входные файлы из входного каталога.
 * $HADOOP_HOME/bin/hadoop jar units.jar hadoop.ProcessUnits input_dir output_dir
 *
 *7  Используем  команду для проверки результирующих файлов в выходной папке.
 * $HADOOP_HOME/bin/hadoop fs -ls output_dir/
 *
 *8  Используем  команду, чтобы увидеть выходные данные в файле Part-00000 . Этот файл создан HDFS.
 * $HADOOP_HOME/bin/hadoop fs -cat output_dir/part-00000
 *
 *
 */

public class WordCount {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

