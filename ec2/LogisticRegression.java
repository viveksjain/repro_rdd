import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.lang.Math;

@SuppressWarnings("deprecation")
public class LogisticRegression {
  public static final String WEIGHTS_FILE = "lr_weights.txt";
  public static final String HDFS_OUTPUT = "/lr_output/part-00000";
  public static final String HDFS_INPUT = "/data/lr_data";
  public static final String JOB_NAME = "LogisticRegression";
  public static final int N_ITERATIONS = 10;
  public static Random random;
  public static DoubleArrayWritable weights;

  private static final int DIM = 10;

  public static class DoubleArrayWritable extends ArrayWritable {
    public static DoubleArrayWritable initRandom() {
      DoubleWritable[] values = new DoubleWritable[DIM];
      if (random == null) random = new Random();
      for (int i = 0; i < DIM; i++) {
        values[i] = new DoubleWritable(2 * random.nextDouble() - 1);
      }
      return new DoubleArrayWritable(values);
    }
    public static DoubleArrayWritable initZero() {
      DoubleWritable[] values = new DoubleWritable[DIM];
      for (int i = 0; i < DIM; i++) values[i] = new DoubleWritable();
      return new DoubleArrayWritable(values);
    }

    public DoubleArrayWritable(DoubleWritable[] values) {
      super(DoubleWritable.class, values);
    }
    @Override
    public DoubleWritable[] get() {
      return (DoubleWritable[]) super.get();
    }
    @Override
    public String toString() {
      DoubleWritable[] values = get();
      String result = "";
      for (int i = 0; i < DIM; i++) {
        result += values[i].toString() + " ";
      }
      return result;
    }
    public double dot(DoubleArrayWritable o) {
      DoubleWritable[] values = get();
      DoubleWritable[] other = o.get();
      double sum = 0;
      for (int i = 0; i < DIM; i++) {
        sum += values[i].get() * other[i].get();
      }
      return sum;
    }
    public void scale(double scaleFactor) {
      DoubleWritable[] values = get();
      for (int i = 0; i < DIM; i++) {
        values[i].set(values[i].get() * scaleFactor);
      }
    }
    public void add(DoubleArrayWritable o) {
      DoubleWritable[] values = get();
      DoubleWritable[] other = o.get();
      for (int i = 0; i < DIM; i++) {
        values[i].set(values[i].get() + other[i].get());
      }
    }
  }

  /* Everything maps to the same key - since we just want to sum all the vectors. */
  public static class Map extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, DoubleArrayWritable> {
    private final static IntWritable one = new IntWritable(1);
    @Override
    public void configure(JobConf job) {
      try {
        // Fetch the file from Distributed Cache Read it and store the
        // centroid in the ArrayList
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
        if (cacheFiles != null && cacheFiles.length > 0) {
          DataInputStream reader = new DataInputStream(new BufferedInputStream(
              new FileInputStream(cacheFiles[0].toString())));
          try {
            weights = DoubleArrayWritable.initZero();
            weights.readFields(reader);
          } finally {
            reader.close();
          }
        }
      } catch (IOException e) {
        System.err.println("Exception reading DistributedCache: " + e);
      }
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter)
        throws IOException {
      String line = value.toString();
      String[] parts = line.split(" ");
      if (parts.length != (DIM + 1)) {
        throw new RuntimeException("Invalid input file - wrong number of dimensions " + parts.length);
      }

      double y = Double.parseDouble(parts[0]);
      DoubleWritable[] values = new DoubleWritable[DIM];
      for (int i = 1; i < DIM; i++) {
        values[i] = new DoubleWritable(Double.parseDouble(parts[i]));
      }
      DoubleArrayWritable x = new DoubleArrayWritable(values);

      x.scale((1 - 1.0/(1+(-y)*Math.exp(weights.dot(x)))) * -y);
      output.collect(one, x);
    }
  }

  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
    private final static IntWritable one = new IntWritable(1);
    @Override
    public void reduce(IntWritable key, Iterator<DoubleArrayWritable> values,
        OutputCollector<IntWritable, DoubleArrayWritable> output, Reporter reporter)
        throws IOException {
      DoubleArrayWritable result = DoubleArrayWritable.initZero();

      while (values.hasNext()) {
        DoubleArrayWritable arr = values.next();
        result.add(arr);
      }

      output.collect(one, result);
    }
  }

  public static void main(String[] args) throws Exception {
    run(args);
  }

  public static void run(String[] args) throws Exception {
    // Initialize random weights
    Path path = new Path(HDFS_OUTPUT);
    FileSystem hdfs = FileSystem.get(new Configuration());
    DataOutputStream out = hdfs.create(path);
    weights.write(out);
    out.close();

    for (int i = 0; i < N_ITERATIONS; i++) {
      JobConf conf = new JobConf(LogisticRegression.class);
      if (i == 0) {
        Path hdfsPath = new Path(WEIGHTS_FILE);
        // upload the file to hdfs. Overwrite any existing copy.
        DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
      } else {
        Path hdfsPath = new Path(HDFS_OUTPUT);
        // upload the file to hdfs. Overwrite any existing copy.
        DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
      }

      conf.setJobName(JOB_NAME);
      conf.setMapOutputKeyClass(IntWritable.class);
      conf.setMapOutputValueClass(DoubleArrayWritable.class);
      conf.setOutputKeyClass(IntWritable.class);
      conf.setOutputValueClass(DoubleArrayWritable.class);
      conf.setMapperClass(Map.class);
      conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(conf,
          new Path(HDFS_INPUT));
      FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT));

      JobClient.runJob(conf);
    }
  }
}