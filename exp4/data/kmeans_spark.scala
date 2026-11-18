import scala.util.control.Breaks._

val k = 3
val maxIter = 20
//读取本地文件，filter过滤掉空行！解决empty String报错
val dataRDD = sc.textFile("file:///home/liu/BigData/exp4/data/points.txt").filter(_.trim.nonEmpty)

val points = dataRDD.map{line=>
  val arr = line.split(",")
  (arr(0).toDouble, arr(1).toDouble)
}.cache()

var centroids = points.take(k)
println("初始中心点：")
centroids.foreach(println)

breakable {
  for(iter <- 1 to maxIter){
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

    val agg = assign.aggregateByKey((0.0,0.0,0))(
      (acc,p) => (acc._1+p._1, acc._2+p._2, acc._3+1),
      (a,b) => (a._1+b._1, a._2+b._2, a._3+b._3)
    )
    val newCent = agg.sortByKey().mapValues{ t=>
      (t._1/t._3, t._2/t._3)
    }.collect().map(_._2)

    var converge = true
    for(i <- centroids.indices){
      if( math.abs(newCent(i)._1 - centroids(i)._1) > 1e-6 ||  math.abs(newCent(i)._2 - centroids(i)._2) >1e-6 ){
        converge = false
      }
    }

    centroids = newCent
    if(converge){
      println(s"迭代第 $iter 次，收敛！")
      break
    }
    println(s"第 $iter 轮迭代完成，本轮中心：")
    centroids.foreach(println)
  }
}

println("====Spark并行K‑Means最终聚类中心====")
centroids.zipWithIndex.foreach{case((x,y),idx)=>
  println(f"簇$idx 中心：($x%.4f,$y%.4f)")
}
