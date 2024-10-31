/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Orkut;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankMapper extends Mapper<Object, Text, IntWritable, NodeWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\s+");

        if (parts.length >= 2) {
            int nodeId = Integer.parseInt(parts[0]);
            List<Integer> edges = new ArrayList<>();

            for (int i = 1; i < parts.length; i++) {
                edges.add(Integer.parseInt(parts[i]));
            }

            NodeWritable node = new NodeWritable(nodeId, edges);
            context.write(new IntWritable(nodeId), node);
        }
    }
}
