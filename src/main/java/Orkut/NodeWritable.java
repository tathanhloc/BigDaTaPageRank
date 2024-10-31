/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package Orkut;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author thanh
 */

public class NodeWritable implements Writable {
    private int nodeId;
    private List<Integer> edges;

    public NodeWritable() {
        edges = new ArrayList<>();
    }

    public NodeWritable(int nodeId, List<Integer> edges) {
        this.nodeId = nodeId;
        this.edges = edges;
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<Integer> getEdges() {
        return edges;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeId);
        out.writeInt(edges.size());
        for (int edge : edges) {
            out.writeInt(edge);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nodeId = in.readInt();
        int size = in.readInt();
        edges = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            edges.add(in.readInt());
        }
    }
}
