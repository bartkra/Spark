import java.io._

object Graph_properties {

  //calculation of directed graph nodes in-degrees and out-degrees (and their averages)

  var deg_res: Array[(String, (Int, Int))] = Array() //result of calculation - Array of (node_id,(indeg,outdeg))
  var deg_av:(Double,Double)=(0.0,0.0) // average (indeg, outdeg)

  def calc_deg(dir:String): Unit ={ //calculates average degree in graph stored in directory 'dir'
    val out = sc.textFile(dir)
      .filter(line => !line.contains("#"))//prepare data
      .flatMap(line => {//for u v emit (u,0,1) and (v,1,0) (map)
        val arr = line.split("\t")
        for (i<- 0 until arr.length) yield{
          val x = arr(i)
          (x,(i,(i+1)%2))
        }
      }).reduceByKey((x1,x2) => (x1._1+x2._1,x1._2+x2._2))//calc sum
    deg_res = out.collect
    val numb = out.count.toDouble
    val av_deg = out.map(elem => (0,elem._2)).reduceByKey((x1,x2) => (x1._1+x2._2,x1._2+x2._2)).collect
    deg_av = ((av_deg(0)._2._1).toDouble/numb,(av_deg(0)._2._2).toDouble/numb)
  }

  def get_deg(dir:String,app:Boolean): Unit ={ //prints degrees of all nodes to file in directory 'dir' (if app=True appends to file)
    val w = new PrintWriter(new FileWriter(dir,app))
    w.println("List of node degrees ([node id] [indeg] [outdeg]):")
    deg_res.foreach(line => w.println( s"${line._1} ${line._2._1} ${line._2._2}"))
    w.close
  }
  
  def get_av_deg(dir:String,app:Boolean): Unit ={//prints average degree to file in directory 'dir' (if app=True appends to file)
    val w = new PrintWriter(new FileWriter(dir,app))
    w.println("Average degrees ([av. indeg] [av. outdeg]):")
    w.println(s"${deg_av._1} ${deg_av._2}")
    w.close
  }

  //calculating of undirected graph nodes clustering coefficients

  var clust_res :Array[(Int, Double, Int)] = Array() //result of calculation - array of ( (node_id,clust. coeff.,degree))
  var clust_av:Double = 0.0

  def calc_clust(dir:String): Unit={// calcultes clustering coefficients for all nodes
    val pairs = sc.textFile(dir) //prepare undirected graph
      .filter(line => !line.contains("#"))
      .map(line => {
        val arr = line.split("\t")
        val x = arr(0).toInt
        val y = arr(1).toInt
        if (x<y){
          ((x,y),0)
        }
        else{
          ((y,x),0)
        }
      })
      .reduceByKey((x1,x2)=>0).keys

    val degrees = pairs.flatMap(el =>{ //calculates node degrees
        val arr = Array(el._1,el._2)
        for (i<- 0 until 2) yield{
          (arr(i),1)
        }
      })
      .reduceByKey(_ + _)
      .sortByKey()

    val degreesArr = degrees.collect//array of node degrees

    val triangles = pairs.map(edge=>{ //calculates clustering coeff. with nodeitr++ algorithm
        if (degreesArr(edge._1 - 1)._2>degreesArr(edge._2 - 1)._2){//put them in order (u,v):deg(u)<deg(v) (map1)
          edge.swap
        }
        else{
          edge
        }
      })
      .groupByKey()
      .flatMap(node=>{//generate all 2-paths (reduce1)
        val arr = (node._2).toArray
        val out = for (i<-0 until arr.length) yield {
          for(j<-(i+1) until arr.length) yield{
            if (arr(j)>arr(i)){
              ((arr(i),arr(j)),node._1)
            }
            else{
              ((arr(j),arr(i)),node._1)
            }
          }
        }
        out.flatten
      })
      .join(pairs.map(x=>(x,-1)))//joins 2-paths with edges (map2)
      .flatMap(x=>Array((x._1._1,1),(x._1._2,1),(x._2._1,1)))//emit (vi,1) for each vi in triangle (reduce 2)
      .reduceByKey(_ + _)//count triangles
      .rightOuterJoin(degrees)//join with degrees
      .map(x=>{
        if (x._2._1.isEmpty){//for nodes with no triagles emit 0
          (x._1,0.0,x._2._2)
        }
        else{
          (x._1,(x._2._1.get.toDouble*2.0)/(x._2._2.toDouble*(x._2._2.toDouble-1)),x._2._2)//calc clustering coeff.
        }
      })
    val numb = triangles.count.toDouble//number of edges
    val av_clust = triangles.map(elem => (0,elem._2)).reduceByKey(_ + _).collect//count sum of all clust. coeff.
    clust_av=av_clust(0)._2/numb//calc. av. clust. coeff.
    clust_res = triangles.collect
  }

  def get_clust(dir:String,app:Boolean): Unit ={//prints clustering coefficients of all nodes to file in directory 'dir' (if app=True appends to file)
    //clust_res.foreach(println)
    val w = new PrintWriter(new FileWriter(dir,app))
    w.println("List of node clustering coeff. ([node id] [clustering coeff.] [degree]):")
    clust_res.foreach(line => w.println(s"${line._1} ${line._2} ${line._3}"))
    w.close
  }

  def get_av_clust(dir:String,app:Boolean): Unit ={//prints average degree to file in directory 'dir' (if app=True appends to file)
    //println(clust_av)
    val w = new PrintWriter(new FileWriter(dir,app))
    w.println("Average clustering coeff:")
    w.println(clust_av)
    w.close
  }

  def run_all(dirin:String,dirout:String): Unit ={ //calculates all
    val w = new PrintWriter(new FileWriter(dirout))
    w.println("Results of calculations:")
    w.close
    calc_deg(dirin)
    get_deg(dirout,true)
    get_av_deg(dirout,true)
    calc_clust(dirin)
    get_clust(dirout,true)
    get_av_clust(dirout,true)
  }

}
