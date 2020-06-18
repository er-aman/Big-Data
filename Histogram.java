import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */

    public Color() {
        this.type = 1;
        this.intensity = 1;
    }

    public Color(short type, short intensity) {
        this.type = type;
        this.intensity = intensity;
    }

    public void write(DataOutput data) throws IOException {
        data.writeShort(type);
        data.writeShort(intensity);
    }

    public void readFields(DataInput data) throws IOException {
        this.type = data.readShort();
        this.intensity = data.readShort();
    }

    public String toString() {
        return Short.toString(type) + " " + Short.toString(intensity);
    }

    public int compareTo(Object c) {
        int comp =  new Short(this.type).compareTo(((Color)c).type);
        if (!(comp == 0))
            return comp;
        return new Short(this.intensity).compareTo(((Color)c).intensity);
    }
}


public class Histogram {
    public class ColorConstants {
        public static final int red = 0;
        public static final short red_type = 1;
        public static final int green = 1;
        public static final short green_type = 2;
        public static final int blue = 2;
        public static final short blue_type = 3;
    }

    public static class HistogramMapper extends Mapper<Object, Text, Color, IntWritable> {
        short red,blue, green;
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            /* write your mapper code */

            String[] data = value.toString().split("[,]");
            red = (Short.parseShort(data[ColorConstants.red]));
            green =(Short.parseShort(data[ColorConstants.green]));
            blue =(Short.parseShort(data[ColorConstants.blue]));
            context.write(new Color(ColorConstants.red_type, red), new IntWritable(1));
            context.write(new Color(ColorConstants.green_type, green), new IntWritable(1));
            context.write(new Color(ColorConstants.blue_type, blue), new IntWritable(1));
        }
    }

    public static class HistogramReducer extends Reducer<Color, IntWritable, Color, LongWritable> {
        @Override
        public void reduce(Color key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            /* write your reducer code */
            int sum = 0;
            for (IntWritable x : values) {
                sum += x.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        /* write your main program code */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);    //Create a new Job
        job.setJarByClass(Histogram.class);

        job.setJobName("Histogram");   //Job specific parameter

        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
