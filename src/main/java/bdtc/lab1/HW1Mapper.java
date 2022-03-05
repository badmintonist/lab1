package bdtc.lab1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

//Определяем тип сообщений


        MessageType message = getTypeMessage(line);
        if (message != null) {
            word.set(message.toString());
            context.write(word, one);
        }
    }


//парсинг строки на наличие сообщений


    public static final MessageType getTypeMessage(String str) {
        str = str.toUpperCase();

        if (str.contains("EMERG") || str.contains("PANIC")) {
            return MessageType.EMERG;
        } else if(str.contains("CRIT")) {
            return MessageType.CRIT;
        } else if(str.contains("ALERT")) {
            return MessageType.ALERT;
        } else if(str.contains("DEBUG")) {
            return MessageType.DEBUG;
        } else if(str.contains("ERROR") || str.contains("ERR")) {
            return MessageType.ERROR;
        } else if(str.contains("INFO")) {
            return MessageType.INFO;
        } else if(str.contains("NOTICE")) {
            return MessageType.NOTICE;
        } else if(str.contains("WARN") || str.contains("WARNING")) {
            return MessageType.WARN;
        } else {
            return null;
        }
    }
}