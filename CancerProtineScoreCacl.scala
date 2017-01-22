import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object AnalyzeTwitters
{
    // Gets Language's name from its code
    def getLangName(code: String) : String =
    {
        return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
    }
    
    def main(args: Array[String]) 
    {
      // val inputFile = args(0)  // uncomment to get command line file name
      
      val inputFile = "/home/najeeb/PDSF99%/DM/final_scoring_table.txt" 
      // val inputFile = "/home/najeeb/PDSF99%/Exercise3/Functions.txt"
      // val inputFile2 = "/home/najeeb/PDSF99%/Exercise3/finalAttributes13.02.txt" 
      val conf = new SparkConf().setMaster("local[*]").setAppName("AnalyzeTwitters3")
      val sc = new SparkContext(conf)
                
        
    // Comment these two lines if you want more verbose messages from Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        val t0 = System.currentTimeMillis
        
        
    println("Application will Generate Output file in current directory---------FunctionCount.txt- FILE----------") 
   
    val file2 = sc.textFile(inputFile2)
    val header = file2.first()
    val data = file2.filter(x => x != header)
    val pro_statusRDD = data.map(_.split(";")).map(x=> (x(0), x(15))) 
    val pro_cancerRDD = pro_statusRDD.filter(_._2 == "cancer") 
    
    val joinedRDD = pro_cancerRDD.join(protineFunRDD)
   
    val fun_proRDD = joinedRDD.map(x=> (x._2._2, x._1))
 
    // res 1 
    val fun_proListRDD = fun_proRDD.groupByKey.map(x=> (x._1, x._2.toList))
      
    // rest 2 

    val fun_cancerCount = fun_proListRDD.map(x=> (x._1, x._2.size)).sortBy(_._1)   
    val distFunctions = protineFunRDD.map(x=> x._2).distinct().map(x=> (x, 0))      
    val combinedRDD = fun_cancerCount.union(distFunctions) 
    val funcSortedByFunction = combinedRDD.groupByKey.map(x=> (x._1, x._2.sum)).sortBy(_._1)
    val funcSortedByCount = combinedRDD.groupByKey.map(x=> (x._1, x._2.sum)).sortBy(_._2, false) 

    //(function, protine)
    val functionProtine = protineFunRDD.map(x=> (x._2, x._1)) 
    val joinedProtine_functionCount = functionProtine.join(funcSortedByFunction).map(x=>(x._2) ).groupByKey.map(x => (x._1, x._2.size, x._2.sum))
    val avgProtineFunctionScore =  joinedProtine_functionCount.map(x=> (x._1, x._2, x._3, x._3/x._2)).sortBy(_._4, false)
    
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("FunctionCount.txt"), "UTF-8"))
    val bw2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("FunctionCountSorted.txt"), "UTF-8"))
    val bw3 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("ProtineFunctionScore.txt"), "UTF-8"))

    // bw.write("Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,RetweetCount,Text\n")  
      // writting output file   
    funcSortedByFunction.collect().foreach{
            x => bw.write(x._1.toString()+ ","+ x._2.toString()+ "\n")   
      }
        
      bw.close
    
    funcSortedByCount.collect().foreach{
            x => bw2.write(x._1.toString()+ ","+ x._2.toString()+ "\n")   
    }
        
 
    bw2.close



    avgProtineFunctionScore.collect().foreach{
          x => bw3.write(x._1.toString()+ ","+ x._2.toString()+ ","+ x._3.toString()+ ", " + x._4.toString()+ "\n")   
      }
        

    bw3.close

        val et = (System.currentTimeMillis - t0) / 1000
        System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
    }
}

