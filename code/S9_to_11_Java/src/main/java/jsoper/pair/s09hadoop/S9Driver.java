/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jsoper.pair.s09hadoop;

import org.apache.hadoop.util.ProgramDriver;

//import jsoper.pair.s09hadoop.MovieCountPerActor;

/**
 * Driver to fire off the MovieCountPerActor mapreduce.
  * 
 * Should use the corresponding run configuration.
 * Output appears in project's output2/final folder
 * 
 * @author john
 *
 */
public class S9Driver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
    	pgd.addClass("pointcleaner", PointCleaner.class, 
    			"A map/reduce program that counts deletes non-contiguous points from bitmap");
    	pgd.driver(argv); 
    }
    catch(Throwable e){
      e.printStackTrace();
    }
	System.out.println("done with Step 9");
    System.exit(exitCode);
  }
  
}
	
