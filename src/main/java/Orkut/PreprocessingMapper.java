package Orkut;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;

public class PreprocessingMapper extends Mapper<Object, Text, IntWritable, NodeWritable> {

    private Map<Integer, Set<Integer>> graph = new HashMap<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\s+");

        if (parts.length >= 2) {
            int fromNode = Integer.parseInt(parts[0]);
            int toNode = Integer.parseInt(parts[1]);

            // Xây dựng đồ thị nếu chưa có
            graph.computeIfAbsent(fromNode, k -> new HashSet<>()).add(toNode);
            graph.computeIfAbsent(toNode, k -> new HashSet<>()).add(fromNode);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Phát ra các node và danh sách cạnh đã được graph hóa
        for (Map.Entry<Integer, Set<Integer>> entry : graph.entrySet()) {
            int nodeId = entry.getKey();
            List<Integer> edges = new ArrayList<>(entry.getValue());

            NodeWritable node = new NodeWritable(nodeId, edges);
            context.write(new IntWritable(nodeId), node);
        }
    }
}
