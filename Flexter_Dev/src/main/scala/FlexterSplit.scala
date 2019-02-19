import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.reflect.io.File

object FlexterSplit {

  def deleteDirectory(directory: String): Unit = { // Function to deleteDirectory
    val dir = File(directory)
    if (dir.isDirectory && dir.exists) {
      dir.deleteRecursively()
    }
  }

  def isDirectoryExists(directory: String): Boolean = { // Function to check if directory exists
    val dir = File(directory)
    if (dir.isDirectory && dir.exists) true else false
  }
  
  def main(args : Array[String])
  {
    if(args.length < 2) // Exception handling to check wether the user is providing both the input and output directories
    {
      System.err.println("Usage: Split Contents <Input-File> <Output-File>")
      System.exit(1);
    }

   
    if (isDirectoryExists(args{0}) == false) // To check if the input directory exists or not
    {
      System.err.println("Input directory doesn't exist")
      System.exit(1);
    }

    val outputLoc =  args{1}+"/topup/"
    val outputCheckLoc = args{1}+"/topup/checkPoint"
    val outputLocUsage = args{1}+"/usage/"
    val outputCheckLocUsage = args{1}+"/usage/checkPoint"
     
     deleteDirectory(outputLoc)
     deleteDirectory(outputLocUsage)


    
    val spark = SparkSession // creating the spark session
                .builder
                .appName("Flextersplit")
                .getOrCreate()
                
                
    val data = spark.readStream // intitiating file streaming
    .format("text")
    .load(args{0}+"/*.tsv")

    val topupFilter = data.filter(data("value").contains("TOPUP")) // filtering contents of file with topup contents
    val usageFilter = data.filter(data("value").contains("USAGE")) // filtering contents of file with usage contents

    val queryUsage = usageFilter.writeStream // streaming output in json format for Usage data
    .outputMode("append")
    .option("startingOffsets", "latest")
    .format("json")
    .option("path", outputLocUsage)
    .option("checkpointLocation", outputCheckLocUsage)
    .start()
    
    val queryTopup = topupFilter.writeStream // streaming output in json format for Topup data
    .format("json")
    .option("startingOffsets", "latest")
    .outputMode("append")
    .option("path", outputLoc)
    .option("checkpointLocation", outputCheckLoc)
    .start
         
    spark.streams.awaitAnyTermination()
     
    
  } 
}
