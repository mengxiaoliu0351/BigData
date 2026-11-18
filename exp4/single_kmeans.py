import math

def read_points(file_path):
    points = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            x,y = line.split(",")
            points.append((float(x), float(y)))
    return points

def euclidean(p1,p2):
    return math.sqrt((p1[0]-p2[0])**2 + (p1[1]-p2[1])**2)

def kmeans(points, k=3, max_iter=20):
    # 选取前k个点作为初始中心点
    centroids = points[:k]
    for _ in range(max_iter):
        clusters = [[] for _ in range(k)]
        for p in points:
            dists = [euclidean(p,c) for c in centroids]
            min_idx = dists.index(min(dists))
            clusters[min_idx].append(p)
        #更新中心点
        new_centroids = []
        for clu in clusters:
            sum_x = sum(p[0] for p in clu)
            sum_y = sum(p[1] for p in clu)
            new_centroids.append((sum_x/len(clu), sum_y/len(clu)))
        if new_centroids == centroids:
            break
        centroids = new_centroids
    return centroids, clusters

if __name__ == "__main__":
    pts = read_points("./data/points.txt")
    cent, clus = kmeans(pts, k=3, max_iter=20)
    print("====单机K‑Means最终聚类中心====")
    for idx,c in enumerate(cent):
        print(f"簇{idx} 中心：({c[0]:.4f},{c[1]:.4f})")
    print("\n====各簇样本====")
    for i,group in enumerate(clus):
        print(f"簇{i}: {group}")
