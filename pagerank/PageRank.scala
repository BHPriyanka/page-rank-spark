package pagerank;

//import wiki.Bz2WikiParser
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io

/**
* Definition of Spark scala class
*/
object PageRank {
   
    def main(args: Array[String]): Unit = {
      /**
     		* Configure the spark sessionand its context
     		*/
        val spark = SparkSession.builder()
          .appName("Spark Page rank implementation")
          .master("local[*]")
          .getOrCreate()
        val sc = spark.sparkContext
        //sc.addJar("s3://scala-page-rank/scala-pagerank-0.0.1-SNAPSHOT.jar")
    
        /**
     		* Set the HADOOP_HOME to include winutils
     		*/
        //System.setProperty("hadoop.home.dir", "C:/winutils");
    
        /**
         * Input: input directory where the bz2 compressed files are placed
         * Output: RDD- RDD[String]
         * Reads the input bz2 file and stores in input.
         */
        val input = sc.textFile(args(0))
  
    
        /**
         * Input: RDD of the bz2 file - RDD[String]
         * Output: (PageName:String,AdjacencyList:List[String])
         * Parses every line from the input RDD calling parseInfo method of Bz2WikiParser
         * Filters out non null entries.
         * Splits the record and form pair RDD key pageName as key and adjacencylist as value
         */
        val  page_rdd = input.
                              map(l => Bz2WikiParser.parseInfo(l)).
                              filter(pageDetail => (pageDetail!=null)).
                              map(page => (page.split("@@@@")(0), getPageLinks(page))).
                              persist()
        
        /**
         * Input: PairRDD - (PageName:String,AdjacencyList:List[String])
         * Output: Number of pairs present
         * Obtain the count of number of entries in pair RDD and compute the initialPage rank
         */
        val numOfNodes = page_rdd.count().toDouble
        val currentPageRank = 1.0/numOfNodes
    
        // Initialise all pages with above computed pagerank
        /**
         * Input: PairRDD - (PageName:String,AdjacencyList:List[String])
         * Output: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
         * Adds the initial page rank value to each page in the pairRDD
         */
        var initialRanks = page_rdd.map(page => (page._1, (page._2, currentPageRank)))
  
    
        /**
         *  Run page rank for 10 times (handle dangling nodes as well)
         */
        for(i <- 1 to 10){
          /**
           * Input: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
           * Output: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
           * Aggregates the sum of page ranks of all dangling nodes/ pages with no outging links
           * This value delta will be used to compute new page ranks for pages.
           */
             val delta = initialRanks.
                                      filter(danglingNode => danglingNode._2._1.length==0).
                                      map{ case (page, (outlinks, pageRank)) => pageRank}.
                                      sum()
             /**
              * Input: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
              * Output: PairRDD - (PageName: String, PageRank: Double)
              * For each outgoing link present in the adjacency list of every node in the graph,
              * Compute new page rank = old_pagerank/size of adjacency list.                    
              */
             val outGoingRanks = initialRanks.flatMap{
							     l => 
							     l._2._1.map(line => (line.trim(), l._2._2/l._2._1.length))}
             
             /**
              * Input: PairRDD - (PageName: String, PageRank: Double)
              * Output: PairRDD - (PageName: String, PageRank: Double)
              * reduceByKey sums the page ranks of pages when the key(pageName) is same.
              * So final pair RDD will have key value pairs of (pageName, pageRank) for every node
              * without any duplications.      
              */
						 val outLinks = outGoingRanks.reduceByKey((x, y) => x + y)
				     						
						 /**
						  * Input: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
						  * Output: PairRDD - (PageName:String,((AdjacencyList:List[String], PageRank:Double), PageRank:Double))
						  * Joins initialRanks and outgoingRanks based on key(pageName). 
						  * The resultant pair RDD contains the combined fields of both pair RDDs.
						  * (List[String],Double) is the adjacency list and its corresponding initial page ranks of pages.
						  * Second Double is the newly computed page rank of every page node.          
						  */
             val joinRanks = initialRanks.join(outLinks)
          
             /**
              * Input: PairRDD - (PageName:String,((AdjacencyList:List[String], PageRank:Double), PageRank:Double))
              * Output: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
              * Each entry in the new pairRDD after join is transformed by calling map and keyBy which will create a
              * RDD[(pageName:String,((adjacenceyList: List[String],pageRank: Double), pageRank:Double))]
              * mapValues ats on only the values of pairRDD and computes the new page rank using alpha and delta for 
              * each page. The existing page Rank value is discarded and initialRanks pairRDD is re initialized with the updated pairRDD
              */
             initialRanks = joinRanks.
                                     map(l => l).keyBy(l => l._1).
		  			                         mapValues(page => (page._2._1._1, (0.15/numOfNodes) + (0.85 * page._2._2) + (delta/numOfNodes)))
         
        }
    
         //Get the top 100 page names along with its page rank values
         /**
         *  Input: PairRDD - (PageName:String,(AdjacencyList:List[String], PageRank:Double))
         *  Output: Array[(String,Double)] : Array[(pageName :String, pageRank: Double)] 
         *  Sorts the records in ascending order of the page rank values using sortBy 
         *  Fetches only top 100, and maps it to the required format which consists of only
         *  page names and thier corresponding page rank values.
         */
         val top100 = initialRanks.
                                   sortBy(top => (-1.0)*top._2._2).
                                   take(100).map(pageNode => {(pageNode._1, pageNode._2._2)})
                 
         /**
          * Input :Array[(String,Double)] : Array[(pageName :String, pageRank: Double)]
          * Writes the output data to the file specified by the command line argument
          */
         sc.parallelize(top100,1).saveAsTextFile(args(1))
         sc.stop()
     }
  
                                      
     // get the adjacency list in the form of a List[String]
    /**
     * Input: pageDetails read from input file - String
     * Output: adjacencyList value - (List[String])
     * Checks if the adjacencylist of the input page node is empty or not.
     * If its empty, create a new List[String]
     * If its not, convert the comma separated string of outgoing links into a list
     */
     def getPageLinks(page : String) = {
      if(page.split("@@@@")(1) != "[]") {
        val length = page.split("@@@@")(1).length()
        (page.split("@@@@")(1).substring(1, length-1).split(",").toList)
      }
      else
        (List[String]())
     }
}
