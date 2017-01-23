/*
This program calculate F-measure, percession and recall on a given attribute 
we provide column number as input and it generates prediction files based on the percentage scoring model using that attribute   
.
*/
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
		
    //  column number in the file 
    val attr =  args(0).toInt  // attribute      
	println("Application will Generate Output file in current directory---------FunctionCount.txt- FILE----------") 
	// reading the scoring table file 
	val file = sc.textFile(inputFile)
				//  (pro#, function)
	val protineFunRDD = file.map(_.split(",")).map(x=> (x(0), x(1))) 
    
    // removing the headers 
    val header = file.first()
    val data = file.filter(x => x != header)
    // filtering the columns, i.e protein_x, class, attribute 
    val tableRDD = data.map(_.split(";")).map(x=> (x(0), x(2), x(attr))) 
    
    // avg for all protine function avg
    val totalCancerFunctionAVG  = tableRDD.map(x=> x._3.toDouble).reduce(_+_)/tableRDD.count  
  
    // fucntion scoring for only cancer related protein
    val cancerPro = tableRDD.filter(x=> x._2 != "0").map(x=> x._3.toFloat)
    // fucntion scoring for only unknown protein
    val nonConerPro = tableRDD.filter(x=> x._2 != "1").map(x=> (x._1, x._2, x._3.toFloat)) 
    // avg for cancer-related-protine  
    val cancerProFunctionAVG  = cancerPro.reduce(_+_)/cancerPro.count 
    

    // mean of the mean i.e. cancerProFunctionAVG and totalCancerFunctionAVG 
    val thresholdFunc =   (cancerProFunctionAVG + totalCancerFunctionAVG)/2   
    

    // measuring the 5% top and belwo values form the mean  
    val unknowList = nonConerPro.map(x=> (x._1, 0))    
    val newCancerU = nonConerPro.filter(_._3 > cancerProFunctionAVG).sortBy(x=> x._3)
    val newCancerD = nonConerPro.filter(_._3 <= cancerProFunctionAVG).sortBy(x=> x._3, false)   
    // we are taking the same percent above and below from the protine having mean value as the percent in the file 
    // geting above the 5% values from the mean
    val uperRDD = sc.parallelize(newCancerU.take(346)) // 346 is the number of 5% of the protines from unknown protein 
    // geting bottom  5% values from the mean 
    val lowerRDD = sc.parallelize(newCancerD.take(346)) // 
    // joining the top 5% and bottom 5% values from the mean 
    val newCancer = uperRDD.union(lowerRDD)  

    // writting output file  
   
    val col = header.split(";")(attr)      
    val outputFile = "/home/najeeb/PDSF99%/DM/outputFiles2/"+col+"Score.txt"     
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8")) 

    newCancer.collect().foreach{
            x => bw.write(x._1.toString()+ ","+ x._2.toString()+ ", " +x._3.toString()+ "\n")   
    }    

    bw.close()

    // comparison of our predicted values with the test 1 file  

    val newCancerRDD = newCancer.map(x=> (x._1, (x._2, x._3)) )
    val test1input = "/home/najeeb/PDSF99%/DM/Test1.txt"
    // reading test 1 file 
    val testfile = sc.textFile(test1input)
    val testrdd = testfile.map(_.split(",")).map{x=> (x(0), x(1)) } 
    // joining test1 file and our predicted protiens to get junction of boht files  
    val joinRDD = newCancerRDD.join(testrdd) 
    
    // filtering cancer proteins from our predicted proteins which are in the test1 file 
    val testCancerCount = joinRDD.filter(x=> x._2._2 =="cancer").count  
    // count of the proteins predicted by our model  
    val nbTestCancerPortine = testrdd.filter(x=> x._2 =="cancer").count 
   
    // cacculating percession, recall 
    val perc = testCancerCount.toFloat / joinRDD.count // 38 /173    
    val recall =  testCancerCount.toFloat /  nbTestCancerPortine  // 38 / 93
    // calculating F measure 
    val fp = (2 * recall * perc) /recall+perc        
 

    // displaying values 
    println(s"Function Score :: precision = $perc , Recall =  $recall , FP =  fp ")
    println(s"Count of the join  :: $joinCount ")  
    println(s"Protines predicted correctly as cancer :: $testCancerCount ") 
    println(s"Nb of cancer Portines in Test File :: $nbTestCancerPortine ") 
    

		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}


