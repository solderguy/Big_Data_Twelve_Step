package jsoper.pair.driver;

import jsoper.pair.s09hadoop.PointCleaner;
import jsoper.pair.s10cluster.KMeansClustering;
import jsoper.pair.s11seqfile.Text2SeqFile;

import org.apache.hadoop.util.ProgramDriver;

/**
 * A Driver to fire off the noise cleanup double mapreduce job
 * 
 * Should use the corresponding run configuration.
 * 
 * @author John Soper
 *
 */

public class HadoopDriver {
    public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
    	pgd.addClass("pointcleaner", PointCleaner.class, 
    			"A map/reduce program that counts deletes non-contiguous points from bitmap");
    	pgd.addClass("kmeans", KMeansClustering.class, 
    			"A Mahout program that performs K-means clustering on bitmaped letters");
    	pgd.addClass("seqfile", Text2SeqFile.class, 
    			"A simple mapreduce program to turn text into a sequencefile");
       	pgd.driver(argv); 
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}