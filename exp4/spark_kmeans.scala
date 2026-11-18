import org.apache.spark.{SparkConf, SparkContext}

object SparkKMeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelKMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val k = 3
    val maxIter = 20
    //读取hdfs数据集
    val dataRDD = sc.textFile("hdfs://localhost:9000/exp4/input/points.txt")
    val points = dataRDD.map{line=>
      val arr = line.split(",")
      (arr(0).toDouble, arr(1).toDouble)
    }.cache()
    //取前k个样本作为初始中心点，和单机版本保持一致，保证结果可对比
    var centroids = points.take(k)

    for(iter <- 1 to maxIter){
      // 每个点分配到距离最近的中心
      val assign = points.map{ p=>
        var minDist = Double.PositiveInfinity
        var bestIdx = 0
        for(i <- centroids.indices){
          val dx = p._1 - centroids(i)._1
          val dy = p._2 - centroids(i)._2
          val dist = math.sqrt(dx*dx + dy*dy)
          if(dist < minDist){
            minDist = dist
            bestIdx = i
          }
        }
        (bestIdx, p)
      }
      //按簇聚合，计算新的中心点 (sumX,sumY,count)
      val agg = assign.aggregateByKey((0.0,0.0,0))(
        (acc,p) => (acc._1+p._1, acc._2+p._2, acc._3+1),
        (a,b) => (a._1+b._1, a._2+b._2, a._3+b._3)
      )
      val newCent = agg.sortByKey().mapValues{ t=>
        (t._1/t._3, t._2/t._3)
      }.collect().map(_._2)

      //收敛判断，中心点不再变化就退出循环
      var converge = true
      for(i <- centroids.indices){
        if(math.abs(newCent(i)._1 - centroids(i)._1) > 1e-6 || math.abs(newCent(i)._2 - centroids(i)._2) >1e-6){
          converge = false
        }
      }
      centroids = newCent
      if(converge){
        println(s"迭代第$iter次，算法收敛，退出")
        break
      }
    }
    println("====Spark并行K‑Means聚类中心====")
    centroids.zipWithIndex.foreach{case((x,y),idx)=>
      println(f"簇$idx 中心：($x%.4f,$y%.4f)")
    }
    sc.stop()
  }
}
