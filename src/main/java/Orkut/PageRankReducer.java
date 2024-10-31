package Orkut;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class PageRankReducer extends Reducer<IntWritable, NodeWritable, IntWritable, DoubleWritable> {
    private static final double DAMPING_FACTOR = 0.85;
    private int totalNodes;  // Biến động để lưu số lượng node

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Đọc số lượng node từ Configuration
        Configuration conf = context.getConfiguration();
        totalNodes = conf.getInt("totalNodes", 5000);  // Nếu không tìm thấy giá trị, sử dụng mặc định là 5000
    }

    @Override
    protected void reduce(IntWritable key, Iterable<NodeWritable> values, Context context) throws IOException, InterruptedException {
        Set<Integer> uniqueEdges = new HashSet<>();
        for (NodeWritable node : values) {
            uniqueEdges.addAll(node.getEdges());
        }

        // Khởi tạo PageRank cho mỗi node
        double rank = (1 - DAMPING_FACTOR) / totalNodes;  // Sử dụng giá trị totalNodes được thiết lập trong setup()
        for (Integer neighbor : uniqueEdges) {
            rank += DAMPING_FACTOR / uniqueEdges.size();
        }

        // Ghi giá trị PageRank cho mỗi node
        context.write(key, new DoubleWritable(rank));
    }
}
