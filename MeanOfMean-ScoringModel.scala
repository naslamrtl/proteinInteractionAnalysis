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
    
    val header = file.first()
    val data = file.filter(x => x != header)
    val tableRDD = data.map(_.split(";")).map(x=> (x(0), x(2), x(7))) 
    // avg for all protine function avg
    val totalCancerFunctionAVG  = tableRDD.map(x=> x._3.toDouble).reduce(_+_)/tableRDD.count  
  
    // fucntion avg for only cancer related pro
    
    val cancerPro = tableRDD.filter(x=> x._2 != "0").map(x=> x._3.toFloat)
    val nonConerPro = tableRDD.filter(x=> x._2 != "1").map(x=> (x._1, x._2, x._3.toFloat)) 
    // avg for cancer-related-protine  
    val cancerProFunctionAVG  = cancerPro.reduce(_+_)/cancerPro.count 
    
    val thresholdFunc =   (cancerProFunctionAVG + totalCancerFunctionAVG)/2   
   
    val newCancer = nonConerPro.filter(_._3> thresholdFunc)

    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("newConcerBasedonFunctionScore.txt"), "UTF-8")) 

    newCancer.collect().foreach{
            x => bw.write(x._1.toString()+ ","+ x._2.toString()+ ", " +x._3.toString()+ "\n")   
    }    

    bw.close()


    val newCancerRDD = newCancer.map(x=> (x._1, (x._2, x._3)) )
    val test1input = "/home/najeeb/PDSF99%/DM/Test1.txt"
    val testfile = sc.textFile(test1input)
    val testrdd = testfile.map(_.split(",")).map{x=> (x(0), x(1)) } 
     
    val joinRDD = newCancerRDD.join(testrdd) 
    
     val testCancerCount = joinRDD.filter(x=> x._2._2 =="cancer").count 

    val nbTestCancerPortine = testrdd.filter(x=> x._2 =="cancer").count
    val perc = testCancerCount.toFloat / joinRDD.count   
    val recall =  testCancerCount.toFloat /  nbTestCancerPortine 
    println(s"Function Score :: precision = $perc , Recall =  $recall")  

    val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}


