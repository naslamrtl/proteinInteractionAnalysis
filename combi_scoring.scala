import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.immutable.ListMap
import scala.io.Source._
import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.io.OutputStream

object Stats{
	val directCNeighbor = scala.collection.mutable.Map[String, Int]()

//meanScoring 
	var falsePositiveMSD = 0
	var trueNegativeMSD = 0
	var truePositiveMSD = 0
	var falseNegativeMSD = 0

//percentageScoring 
	var correctlyPredictedPSD = 0
	var falsePositivePSD = 0
	var trueNegativePSD = 0
	var truePositivePSD = 0
	var falseNegativePSD = 0


  	def main(args: Array[String]) {
  		val conf = new SparkConf().setAppName("appName").setMaster("local")
		val sc = new SparkContext(conf)

//Input: test file
		val scoringBasisTest = sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/scoringModel/Test_final_scoring_table.csv").map(x => x.split(";")).map(x => (x(0).toString, (x(1).toInt)))
//Input: mean scoring predictions
		val  betweennessCentrality	= sc.textFile( //1
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/BetweennessCentralityScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  degree	= sc.textFile(	//2
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/DegreeScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  topologicalCoefficient	= sc.textFile(	//3
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/TopologicalCoefficientScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  numberOfCancerousNeighbors	= sc.textFile(	//4
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/NumberOfCancerousNeighborsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  functionScoringValueSum	= sc.textFile(	//5
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/FunctionScoringValueSumScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  numberOfCancerousFunctions	= sc.textFile(	//6
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/NumberOfCancerousFunctionsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  numberOfFunctions	= sc.textFile(	//7
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/NumberOfFunctionsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  functionScoringValueAverage	= sc.textFile( //8
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/FunctionScoringValueAverageScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  ratioOfCancerousNeighbors	= sc.textFile( //9
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/RatioOfCancerousNeighborsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  ratioOfCancerousFunctions	= sc.textFile( //10
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/RatioOfCancerousFunctionsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

//Input: percentage scoring predictions
		val  betweennessCentralityP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/BetweennessCentralityScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  degreeP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/DegreeScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  topologicalCoefficientP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/TopologicalCoefficientScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  numberOfCancerousNeighborsP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/NumberOfCancerousNeighborsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  functionScoringValueSumP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/FunctionScoringValueSumScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  numberOfCancerousFunctionsP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/NumberOfCancerousFunctionsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  numberOfFunctionsP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/NumberOfFunctionsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  functionScoringValueAverageP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/FunctionScoringValueAverageScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  ratioOfCancerousNeighborsP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/RatioOfCancerousNeighborsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
		val  ratioOfCancerousFunctionsP	= sc.textFile(
			"/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/percentageScoring/RatioOfCancerousFunctionsScore.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

//-------------------------------------------------------------------------------------------------------------------------------------------------------------------
//Combi_scoring: mean_scoring_predictions
		val fileJoin =  betweennessCentrality.join(degree).map(x => (x._1, (x._2._1, x._2._2)))
		.join(topologicalCoefficient).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._2)))
		.join(numberOfCancerousNeighbors).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)))
		.join(functionScoringValueSum).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2)))
		.join(numberOfCancerousFunctions).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5, x._2._2)))
		.join(numberOfFunctions).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6, x._2._2)))
		.join(functionScoringValueAverage).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7, x._2._2)))
		.join(ratioOfCancerousNeighbors).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._2._1._8, x._2._2)))
		.join(ratioOfCancerousFunctions).map(x => (x._1, x._2._1._1.toInt, x._2._1._2.toInt, x._2._1._3.toInt, x._2._1._4.toInt,x._2._1._5.toInt,x._2._1._6.toInt,x._2._1._7.toInt,x._2._1._8.toInt,x._2._1._9.toInt, x._2._2.toInt))
	
		val combiStats = fileJoin.map(x => {
			if((x._2 +x._3+ x._4+ x._5+ x._6+ x._7+ x._8+ x._9+ x._10+ x._11).toInt >= 7){

				(x._1, 1)
			}else{
				(x._1, 0)
			}
			}).map(x => (x._1.toString, (x._2.toInt)))

		val bw = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/Combi_meanScoring.txt"))
					for(x <- combiStats.collect()){
						bw.write(x._1 + "," + x._2.toInt + "\n")
					}
					bw.close()
		val combiStats2 = sc.textFile("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/Combi_meanScoring.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

//-------------------------------------------------------------------------------------------------------
//Combi_scoring: percentage_scoring_predictions
		val fileJoinP =  betweennessCentralityP.join(degreeP).map(x => (x._1, (x._2._1, x._2._2)))
		.join(topologicalCoefficientP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._2)))
		.join(numberOfCancerousNeighborsP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)))
		.join(functionScoringValueSumP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2)))
		.join(numberOfCancerousFunctionsP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5, x._2._2)))
		.join(numberOfFunctionsP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6, x._2._2)))
		.join(functionScoringValueAverageP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7, x._2._2)))
		.join(ratioOfCancerousNeighborsP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._2._1._8, x._2._2)))
		.join(ratioOfCancerousFunctionsP).map(x => (x._1, x._2._1._1.toInt, x._2._1._2.toInt, x._2._1._3.toInt, x._2._1._4.toInt,x._2._1._5.toInt,x._2._1._6.toInt,x._2._1._7.toInt,x._2._1._8.toInt,x._2._1._9.toInt, x._2._2.toInt))
	
		val combiStatsP = fileJoinP.map(x => {
			if((x._2 +x._3+ x._4+ x._5+ x._6+ x._7+ x._8+ x._9+ x._10+ x._11).toInt >= 2){
				(x._1, 1)
			}else{
				(x._1, 0)
			}
			}).map(x => (x._1.toString, (x._2.toInt)))

		val bwP = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiP_percentageScoring.txt"))
					for(x <- combiStatsP.collect()){
						bwP.write(x._1 + "," + x._2.toInt + "\n")
					}
					bwP.close()
		val combiStats2P = sc.textFile("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiP_percentageScoring.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

//-------------------------------------------------------------------------------------------------------
//Combi_scoring: all_predictions
		val fileJoinTotal = fileJoin.map(x => (x._1, (x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10, x._11))).join(fileJoinP.map(x => (x._1, (x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11))))
			.map(x => (x._1, x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._2._1._8,x._2._1._9,x._2._1._10,
							x._2._2._1,x._2._2._2,x._2._2._3,x._2._2._4,x._2._2._5,x._2._2._6,x._2._2._7,x._2._2._8,x._2._2._9,x._2._2._10))

		val combiStatsTotal = fileJoinTotal.map(x => {
			if((x._2 +x._3+ x._4+ x._5+ x._6+ x._7+ x._8+ x._9+ x._10+ x._11+ x._12+ x._13+ x._14+ x._15+ x._16+ x._17+ x._18+ x._19+ x._20+ x._21).toInt>=8){
				(x._1, 1)
			}else{
				(x._1, 0)
			}
			}).map(x => (x._1.toString, (x._2.toInt)))

		val bwTotal = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiTotal_Scoring.txt"))
					for(x <- combiStatsTotal.collect()){
						bwTotal.write(x._1 + "," + x._2.toInt + "\n")
					}
					bwTotal.close()
		val combiStats2Total = sc.textFile("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiTotal_Scoring.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

		val bwTotalo = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/allAttribute_Scoring.txt"))
					for(x <- fileJoinTotal.collect()){
						bwTotalo.write(x._1 + "," + x._2.toInt+ "," + x._3+ "," + x._4+ "," + x._5+ "," + x._6+ "," + x._7+ "," + x._8+ "," + x._9+ "," + x._10+ "," + x._11+ "," + x._12+ "," + x._13
						+ "," + x._14+ "," + x._15+ "," + x._16+ "," + x._17+ "," + x._18+ "," + x._19+ "," + x._20+ "," + x._21+ "\n")
					}
					bwTotalo.close()

//-------------------------------------------------------------------------------------------------------
//Combi_scoring: best_3_Fmeasure
		val fileJoinBestF = numberOfCancerousNeighbors.join(numberOfCancerousFunctions).join(numberOfCancerousNeighborsP)
				.map(x => (x._1, x._2._1._1, x._2._1._2, x._2._2))

		val combiStatsBestF = fileJoinBestF.map(x => {
			if((x._2.toInt+ x._3.toInt+ x._4.toInt)>=2){ 
				(x._1, 1)
			}else{
				(x._1, 0)
			}
			}).map(x => (x._1.toString, (x._2.toInt)))

		val bwBestF = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiBestF_Scoring.txt"))
					for(x <- combiStatsBestF.collect()){
						bwBestF.write(x._1 + "," + x._2.toInt + "\n")
					}
					bwBestF.close()

		val combiStats2BestF = sc.textFile("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiBestF_Scoring.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

//-------------------------------------------------------------------------------------------------------
//Combi_scoring: best_5_Fmeasure
		val fileJoinBestF5 = numberOfCancerousNeighbors.join(numberOfCancerousFunctions)
				.join(numberOfCancerousNeighborsP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._2)))
				.join(numberOfFunctions).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)))
				.join(functionScoringValueSum).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))

		val combiStatsBestF5 = fileJoinBestF5.map(x => {
			if((x._2.toInt+ x._3.toInt+ x._4.toInt+ x._5.toInt+ x._6.toInt)>=4){ 
				(x._1, 1)
			}else{
				(x._1, 0)
			}
			}).map(x => (x._1.toString, (x._2.toInt)))

		val bwBestF5 = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiBestF5_Scoring.txt"))
					for(x <- combiStatsBestF5.collect()){
						bwBestF5.write(x._1 + "," + x._2.toInt + "\n")
					}
					bwBestF5.close()

		val combiStats2BestF5 = sc.textFile("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiBestF5_Scoring.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))

//-------------------------------------------------------------------------------------------------------
//Combi_scoring: best_10_Fmeasure
		val fileJoinBestF10 = numberOfCancerousNeighbors.join(numberOfCancerousFunctions)
				.join(numberOfCancerousNeighborsP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._2)))
				.join(numberOfFunctions).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)))
				.join(functionScoringValueSum).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2)))
				.join(betweennessCentrality).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5, x._2._2)))
				.join(ratioOfCancerousNeighbors).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6, x._2._2)))
				.join(degree).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7, x._2._2)))
				.join(betweennessCentralityP).map(x => (x._1, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4,x._2._1._5,x._2._1._6,x._2._1._7,x._2._1._8, x._2._2)))
				.join(topologicalCoefficient).map(x => (x._1, x._2._1._1.toInt, x._2._1._2.toInt, x._2._1._3.toInt, x._2._1._4.toInt,x._2._1._5.toInt,x._2._1._6.toInt,x._2._1._7.toInt,x._2._1._8.toInt,x._2._1._9.toInt, x._2._2.toInt))
			

		val combiStatsBestF10 = fileJoinBestF10.map(x => {
			if((x._2.toInt+ x._3.toInt+ x._4.toInt+ x._5.toInt+ x._6.toInt+ x._7+ x._8+ x._9+ x._10+ x._11)>=6){ 
				(x._1, 1)
			}else{
				(x._1, 0)
			}
			}).map(x => (x._1.toString, (x._2.toInt)))

		val bwBestF10 = new BufferedWriter(new FileWriter("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiBestF10_Scoring.txt"))
					for(x <- combiStatsBestF10.collect()){
						bwBestF10.write(x._1 + "," + x._2.toInt + "\n")
					}
					bwBestF10.close()

		val combiStats2BestF10 = sc.textFile("/home/jonathan/ws1617/DBaDM/4thAssignment/inputStats/CombiBestF10_Scoring.txt").map(x => x.split(",")).map(x => (x(0).toString, (x(1))))
//-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
//Statistic generation

		val files = Map(betweennessCentrality -> "MbetweennessCentrality", 
							degree -> "Mdegree",
							topologicalCoefficient -> "MtopologicalCoefficient",
							numberOfCancerousNeighbors -> "MnumberOfCancerousNeighbors", 
							functionScoringValueSum -> "MfunctionScoringValueSum",
							numberOfCancerousFunctions -> "MnumberOfCancerousFunctions", 
							numberOfFunctions -> "MnumberOfFunctions",
							functionScoringValueAverage -> "MfunctionScoringValueAverage",
							ratioOfCancerousNeighbors -> "MratioOfCancerousNeighbors",
							ratioOfCancerousFunctions -> "MratioOfCancerousFunctions",

							betweennessCentralityP -> "PbetweennessCentrality", 
							degreeP -> "Pdegree",
							topologicalCoefficientP -> "PtopologicalCoefficient",
							numberOfCancerousNeighborsP -> "PnumberOfCancerousNeighbors", 
							functionScoringValueSumP -> "PfunctionScoringValueSum",
							numberOfCancerousFunctionsP -> "PnumberOfCancerousFunctions", 
							numberOfFunctionsP-> "PnumberOfFunctions",
							functionScoringValueAverageP -> "PfunctionScoringValueAverage",
							ratioOfCancerousNeighborsP -> "PratioOfCancerousNeighbors",
							ratioOfCancerousFunctionsP -> "PratioOfCancerousFunctions",
							
							combiStats2 -> "combi_mean",
							combiStats2P -> "combi_percentage",
							combiStats2Total -> "combi_all_",
							combiStats2BestF -> "combi_best3Fmeasure",
							combiStats2BestF5 -> "combi_best5Fmeasure",
							combiStats2BestF10 -> "combi_best10Fmeasure"
							)

		files.foreach(x => {
				val attribute = x._2	
				val file = scoringBasisTest.map(x => (x._1.toString, (x._2.toInt))).join(x._1.map(x => (x._1.toString, (x._2.toInt))))

				file.foreach(x => {
								//	predictedClass = cancer && realClass == noCancer	==> falseNegative
							if (x._2._1 == 1 && x._2._2 ==0) {
								this.falseNegativeMSD += 1
								//predictedClass = noCancer && realClass == cancer 		==> falsePositive
							}else if (x._2._1 == 0 && x._2._2 ==1) {
								this.falsePositiveMSD += 1
								//predictedClass = cancer %% realClass == cancer 		==> truePositive
							}else if (x._2._1 == 1 && x._2._2 ==1) {
								this.truePositiveMSD += 1
								//predictedClass = noCancer %% realClass == noCancer 	==> trueNegative
							}else if (x._2._1 == 0 && x._2._2 ==0) {
								this.trueNegativeMSD += 1
							}
						})

				var precisionMSD = (this.truePositiveMSD.toFloat/(this.truePositiveMSD + this.falsePositiveMSD).toFloat)
				var recallMSD = (this.truePositiveMSD.toFloat/(this.truePositiveMSD + this.falseNegativeMSD).toFloat)
				var fmeasureMSD = ((2*precisionMSD)* recallMSD).toFloat/(precisionMSD + recallMSD).toFloat
				var count = x._1.count()

//--------------------------------------------------------------------------------------------------------------------------------------------------------------------				
//Printing
				val bwStats = new BufferedWriter(new FileWriter(s"/home/jonathan/ws1617/DBaDM/4thAssignment/outputStats/$attribute _scoringStats.txt"))
							bwStats.write(
								s"$attribute mean scoring: \n" +											
								"precision: " + precisionMSD +"\n" +	
								"recall: " + recallMSD +"\n" +		
								"fmeasure: " + fmeasureMSD + "\n" +
								"count: " + count +"\n" +
								"correctly predicted: " + (trueNegativeMSD + truePositiveMSD) +"\n" +
								"wrongly predicted: " + (falseNegativeMSD + falsePositiveMSD) +"\n" +
								"true negatives: " + trueNegativeMSD + "\n" +
								"true positives: " + truePositiveMSD + "\n" +
								"false positives: " + falsePositiveMSD + "\n" +
								"false negatives: " + falseNegativeMSD + "\n \n" 
								)
					bwStats.close()

					val write = new PrintWriter(new FileOutputStream(new File("/home/jonathan/ws1617/DBaDM/4thAssignment/outputStats/statsCombi.txt"),true))
					write.write(
						s"$attribute" +  ";" + precisionMSD + ";" + recallMSD + ";" + fmeasureMSD + ";" + 
						count + ";" + (trueNegativeMSD + truePositiveMSD) + ";" + (falsePositiveMSD+falseNegativeMSD) + ";" +
						trueNegativeMSD + ";" + truePositiveMSD + ";" + falsePositiveMSD + ";" + falseNegativeMSD + "\n"
						)
					write.close()

					falsePositiveMSD = 0
					trueNegativeMSD = 0
					truePositiveMSD = 0
					falseNegativeMSD = 0
		})
	  	sc.stop()
	}
}
