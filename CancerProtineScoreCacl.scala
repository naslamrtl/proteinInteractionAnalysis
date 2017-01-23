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
    
    // reading function file  
    val file = sc.textFile(inputFile)
                //  (pro#, function)
    val protineFunRDD = file.map(_.split(",")).map(x=> (x(0), x(1)))  
    // reading final scoring table 
    val file2 = sc.textFile(inputFile2)
    // removing headers 
    val header = file2.first()
    val data = file2.filter(x => x != header)
    val pro_statusRDD = data.map(_.split(";")).map(x=> (x(0), x(15))) 
    // filtering cancerous protein
    val pro_cancerRDD = pro_statusRDD.filter(_._2 == "cancer") 
    // joining function rdd with cancerous protine RDD
    val joinedRDD = pro_cancerRDD.join(protineFunRDD)
   
    // maping fucntion with the protein performing that function, there will be more entries for one function     
    val fun_proRDD = joinedRDD.map(x=> (x._2._2, x._1))
 
    // res 1 i.e group one function with all of proteins (as list) performing that function 
    val fun_proListRDD = fun_proRDD.groupByKey.map(x=> (x._1, x._2.toList))
      
    // rest 2 
    // calculating the total number of protines for each function 
    val fun_cancerCount = fun_proListRDD.map(x=> (x._1, x._2.size)).sortBy(_._1)   
    // list of distick functions 
    val distFunctions = protineFunRDD.map(x=> x._2).distinct().map(x=> (x, 0))      
    // combining all the functions having any protein or not as its performer 
    val combinedRDD = fun_cancerCount.union(distFunctions) 
    // sorting by function name 
    val funcSortedByFunction = combinedRDD.groupByKey.map(x=> (x._1, x._2.sum)).sortBy(_._1)
    // sorting by number of protiens for each function 
    val funcSortedByCount = combinedRDD.groupByKey.map(x=> (x._1, x._2.sum)).sortBy(_._2, false) 
 
         
    // NOW calculation score for each protine based on functions performed by this protein       
    //(function, protine)
    // mapping protineFunRDD (pro, Func)  ==>>  (Fun, protein)
    val functionProtine = protineFunRDD.map(x=> (x._2, x._1)) 
    // joining above wiht the function scoring RDD that we calculated above, 
    // grouped each protines with all of its function scores
    // then we calculated count of function and sum of function score 
    val joinedProtine_functionCount = functionProtine.join(funcSortedByFunction).map(x=>(x._2) ).groupByKey.map(x => (x._1, x._2.size, x._2.sum))
    // calculating the average of the function score for each protein and sorting it in dec order 
    val avgProtineFunctionScore = joinedProtine_functionCount.map(x=> (x._1, x._2, x._3, x._3/x._2)).sortBy(_._4, false)
    
    // writing output files 
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

