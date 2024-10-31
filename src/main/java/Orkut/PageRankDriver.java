package Orkut;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class PageRankDriver {
     public static void main(String[] args) throws Exception {
          if (args.length != 2) {
               System.err.println("Usage: PageRankDriver <input path> <output path>");
               System.exit(-1);
          }

          // Thiết lập cấu hình Hadoop
          Configuration conf = new Configuration();

          // Tính toán số lượng node từ file đầu vào
          int totalNodes = calculateTotalNodes(args[0], conf);
          System.out.println("Total number of nodes: " + totalNodes);

          // Truyền số lượng node vào cấu hình của job
          conf.setInt("totalNodes", totalNodes);

          Job job = Job.getInstance(conf, "PageRank Processing");

          // Thiết lập các lớp Mapper, Reducer và các thiết lập khác cho job
          job.setJarByClass(PageRankDriver.class);
          job.setMapperClass(PreprocessingMapper.class);
          job.setReducerClass(PageRankReducer.class);

          job.setMapOutputKeyClass(IntWritable.class);
          job.setMapOutputValueClass(NodeWritable.class);
          job.setOutputKeyClass(IntWritable.class);
          job.setOutputValueClass(DoubleWritable.class);

          FileInputFormat.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));

          job.setNumReduceTasks(1); // Đảm bảo chỉ có một reducer để có một file đầu ra duy nhất

          System.exit(job.waitForCompletion(true) ? 0 : 1);
     }

     // Hàm tính toán tổng số lượng node từ file đầu vào
     private static int calculateTotalNodes(String inputPath, Configuration conf) throws IOException {
          Set<Integer> nodes = new HashSet<>();

          // Sử dụng FileSystem của Hadoop để đọc file từ HDFS
          FileSystem fs = FileSystem.get(conf);
          Path path = new Path(inputPath);

          try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
               String line;

               // Đọc file và thêm tất cả các node vào HashSet để đếm các node duy nhất
               while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    if (parts.length >= 2) {
                         nodes.add(Integer.parseInt(parts[0]));
                         nodes.add(Integer.parseInt(parts[1]));
                    }
               }
          }

          return nodes.size(); // Trả về số lượng node duy nhất
     }
}
