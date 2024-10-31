package Orkut;

import java.io.*;
import java.util.*;

import java.io.*;
import java.util.*;

public class LocalPageRank {
    private static final double DAMPING_FACTOR = 0.85;
    private static final int ITERATIONS = 10;

    public static void main(String[] args) throws IOException {
        // Đường dẫn tới file đầu vào
        String inputFile = "D:/BigData/Data/data500/com-orkut.ungraph.txt";

        // Đọc đồ thị từ file
        Map<Integer, List<Integer>> graph = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\s+");
            int from = Integer.parseInt(parts[0]);
            int to = Integer.parseInt(parts[1]);

            // Thêm vào đồ thị
            graph.computeIfAbsent(from, k -> new ArrayList<>()).add(to);
        }
        reader.close();

        // Khởi tạo PageRank cho mỗi node
        Map<Integer, Double> pageRank = new HashMap<>();
        for (Integer node : graph.keySet()) {
            pageRank.put(node, 1.0 / graph.size());
        }

        // Chạy thuật toán PageRank
        for (int i = 0; i < ITERATIONS; i++) {
            Map<Integer, Double> newPageRank = new HashMap<>();

            // Tính toán PageRank mới cho mỗi node
            for (Integer node : graph.keySet()) {
                double rank = (1 - DAMPING_FACTOR) / graph.size();
                for (Integer neighbor : graph.keySet()) {
                    if (graph.get(neighbor).contains(node)) {
                        rank += DAMPING_FACTOR * pageRank.get(neighbor) / graph.get(neighbor).size();
                    }
                }
                newPageRank.put(node, rank);
            }

            // Cập nhật PageRank
            pageRank = newPageRank;
        }

        // In ra kết quả
        for (Map.Entry<Integer, Double> entry : pageRank.entrySet()) {
            System.out.println("Node: " + entry.getKey() + " PageRank: " + entry.getValue());
        }
    }
}
