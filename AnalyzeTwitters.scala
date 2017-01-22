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
        
        // Add your code here
        
    println("Application will Generate Output file in current directory---------FunctionCount.txt- FILE----------") 
    // reading the file 
    val file = sc.textFile(inputFile)
                //  (pro#, function)
    val protineFunRDD = file.map(_.split(",")).map(x=> (x(0), x(1))) 
    
    val attr =  args(0).toInt  // attribute 
    println(attr)
    val header = file.first()
    val data = file.filter(x => x != header)
    val tableRDD = data.map(_.split(";")).map(x=> (x(0), x(2), x(attr))) 
    // avg for all protine function avg
    val totalCancerFunctionAVG  = tableRDD.map(x=> x._3.toDouble).reduce(_+_)/tableRDD.count  
  
    // fucntion avg for only cancer related pro
    
    val cancerPro = tableRDD.filter(x=> x._2 != "0").map(x=> x._3.toFloat)
    val nonConerPro = tableRDD.filter(x=> x._2 != "1").map(x=> (x._1, x._2, x._3.toFloat)) 
    // avg for cancer-related-protine  
    val cancerProFunctionAVG  = cancerPro.reduce(_+_)/cancerPro.count  
    
    

    val thresholdFunc =   (cancerProFunctionAVG + totalCancerFunctionAVG)/2   
    

    val unknowList = nonConerPro.map(x=> (x._1, 0))
    
    val newCancerU = nonConerPro.filter(_._3 > cancerProFunctionAVG).sortBy(x=> x._3)
    val newCancerD = nonConerPro.filter(_._3 <= cancerProFunctionAVG).sortBy(x=> x._3, false)   
    val uperRDD = sc.parallelize(newCancerU.take(346))
    val lowerRDD = sc.parallelize(newCancerD.take(346))
    val newCancer = uperRDD.union(lowerRDD)  



   // scoring 1, method
// comment below and uncomment above 2 to run scoring method 2   

   /*var newCancer = nonConerPro.filter(_._3 >= thresholdFunc) 

   
   if (thresholdFunc > cancerProFunctionAVG ){
     newCancer = nonConerPro.filter(_._3 <= thresholdFunc)
     println("Taking less than values ")  
   }
*/
  
   val newCancerKeyVal = newCancer.map(x=> (x._1, 1))
   val nonCancerRDD = unknowList.subtractByKey(newCancerKeyVal)
   val combinedRDD = newCancerKeyVal.union(nonCancerRDD) 

   /*if (thresholdFunc < cancerProFunctionAVG ) {
     val newCancer = nonConerPro.filter(_._3 >= thresholdFunc)
   }else{
      
   val newCancer = nonConerPro.filter(_._3 <= thresholdFunc)
   }*/

    val col = header.split(";")(attr)      
    val outputFile = "/home/najeeb/PDSF99%/DM/outputFiles2/"+col+"Score.txt"     

    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8")) 

    val bw1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("ScoringModel1.txt"), "UTF-8")) 

    val a = newCancer.map(x=> (x._1, 1))
    combinedRDD.collect().foreach{
            x => bw.write(x._1.toString()+ ","+ x._2.toString()+ "\n")   
    }    

    bw.close()


    val newCancerRDD = newCancer.map(x=> (x._1, (x._2, x._3)) )
    val test1input = "/home/najeeb/PDSF99%/DM/Test1.txt"
    val testfile = sc.textFile(test1input)
    val testrdd = testfile.map(_.split(",")).map{x=> (x(0), x(1)) } 
     
    val joinRDD = newCancerRDD.join(testrdd) 
    val joinCount =  joinRDD.count    
    val testCancerCount = joinRDD.filter(x=> x._2._2 =="cancer").count  

    val nbTestCancerPortine = testrdd.filter(x=> x._2 =="cancer").count 
    val perc = testCancerCount.toFloat / joinRDD.count // 38 /173    
    val recall =  testCancerCount.toFloat /  nbTestCancerPortine  // 38 / 93
    
    val fp = (2 * recall * perc) / recall + perc    

    

    bw1.write(s"$col :: precision = $perc , Recall =  $recall , FP =  $fp ")
    bw1.close()
    println(s"$col :: precision = $perc , Recall =  $recall , FP =  $fp ")
    println(s"Count of the join  :: $joinCount ")  
    println(s"Protines predicted correctly as cancer :: $testCancerCount ") 
    println(s"Nb of cancer Portines in Test File :: $nbTestCancerPortine ") 
    println(s" Mean of Mean  = $thresholdFunc")

    /*val file2 = sc.textFile(inputFile2)
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
*/
        val et = (System.currentTimeMillis - t0) / 1000
        System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
    }
}

