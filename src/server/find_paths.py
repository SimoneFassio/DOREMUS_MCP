import csv
import sys
import heapq
from collections import defaultdict

# Usage: python find_paths.py start_node end_node k

def load_graph(csv_path):
    graph = defaultdict(list)
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 3:
                continue
            subj, pred, obj = row[:3]
            graph[subj].append((pred, obj))
    return graph

# Find top k shortest simple paths (Yen's algorithm-like)
def find_k_shortest_paths(graph, start, end, k):
    def dijkstra_path(graph, start, end, banned_edges, banned_nodes):
        heap = []
        heapq.heappush(heap, (0, start, []))
        visited = set()
        while heap:
            cost, node, path = heapq.heappop(heap)
            if node == end:
                return path
            if node in visited:
                continue
            visited.add(node)
            for pred, neighbor in graph.get(node, []):
                if (node, pred, neighbor) in banned_edges or neighbor in banned_nodes or neighbor in [n for _, _, n in path]:
                    continue
                heapq.heappush(heap, (cost + 1, neighbor, path + [(node, pred, neighbor)]))
        return None

    A = []
    B = []
    path = dijkstra_path(graph, start, end, set(), set())
    if not path:
        return []
    A.append(path)
    for k_i in range(1, k):
        for i in range(len(A[-1])):
            spur_node = A[-1][i][0]
            root_path = A[-1][:i]
            banned_edges = set()
            banned_nodes = set()
            for p in A:
                if p[:i] == root_path and len(p) > i:
                    banned_edges.add(p[i])
            for n, _, _ in root_path:
                banned_nodes.add(n)
            spur_path = dijkstra_path(graph, spur_node, end, banned_edges, banned_nodes)
            if spur_path:
                total_path = root_path + spur_path
                if total_path not in B:
                    B.append(total_path)
        if not B:
            break
        B.sort(key=len)
        A.append(B.pop(0))
    return A

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python find_paths.py <csv_file> <start_node> <end_node> <k>")
        sys.exit(1)
    csv_path, start_node, end_node, k = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    graph = load_graph(csv_path)
    paths = find_k_shortest_paths(graph, start_node, end_node, k)
    for i, path in enumerate(paths, 1):
        print(f"Path {i} (length {len(path)}):")
        for triple in path:
            print(f"  {triple[0]} --[{triple[1]}]--> {triple[2]}")
        print()
    if not paths:
        print("No path found.")
