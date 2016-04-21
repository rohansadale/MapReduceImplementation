import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.Charset;
import java.lang.System;
import java.lang.Runnable;
import java.util.concurrent.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.thrift.protocol.TBinaryProtocol;

public class ComputeServiceHandler implements ComputeService.Iface{

		private String INPUT_DIRECTORY_KEY = "";
		private String INTERMEDIATE_DIRECTORY_KEY = "";
		private String OUTPUT_DIRECTORY_KEY = "";
		private float failProbability = (float)0.0;	
		private HashMap<String, Boolean> sortState;
		private HashMap<String, Boolean> mergeState;
		private HashMap<String, String> sortFileMap;
		private HashMap<String, String> mergeFileMap;
		private HashMap<String, List<String>> mergeDelete;
		private HashMap<String, Long> jobDuration;
		private int Proactive = 0;
		Random rnd                          = new Random();

		public ComputeServiceHandler(String INPUT_DIRECTORY_KEY, String INTERMEDIATE_DIRECTORY_KEY, String OUTPUT_DIRECTORY_KEY,float failProbability,int Proactive){
				this.INPUT_DIRECTORY_KEY 	= INPUT_DIRECTORY_KEY;
				this.INTERMEDIATE_DIRECTORY_KEY = INTERMEDIATE_DIRECTORY_KEY;	
				this.OUTPUT_DIRECTORY_KEY 	= OUTPUT_DIRECTORY_KEY;	
				this.failProbability 		= failProbability;
				this.sortState				= new HashMap<String, Boolean>();
				this.mergeState				= new HashMap<String, Boolean>();
				this.sortFileMap			= new HashMap<String,String>();
				this.mergeFileMap			= new HashMap<String,String>();				
				this.mergeDelete 			= new HashMap<String, List<String>>();
				this.jobDuration 			= new HashMap<String, Long>();
				this.Proactive 				= Proactive;
		}


		// Sort Task
		@Override
				public JobTime doSort(String fileName, int offset, int count,String file_id)
				{
						float curProbability				= rnd.nextFloat();
						if(curProbability  < this.failProbability)
						{
								System.out.println("Failing this Task as random number generated is less than fail probability ...");
								return new JobTime("",(long)-1);
						}

						System.out.println("\nStarting Sort task for " + fileName);
						long startTime = System.currentTimeMillis();
						String resultFileName	 = "";
						try
						{
								RandomAccessFile file = new RandomAccessFile(INPUT_DIRECTORY_KEY + fileName, "r");
								if(offset > 0)
										file.seek(offset-1);		
								else
										file.seek(offset);		

								boolean startFlag = true;
								byte [] tempStart = new byte[2];		
								file.read(tempStart);

								// 32 is ASCII value of space		
								if(tempStart[0] == 32 || tempStart[1] == 32 || offset == 0)
										startFlag = false;

								// Read required number of bytes
								file.seek(offset);
								byte[] bytes = new byte[count];
								file.read(bytes);
								String s = new String(bytes, "UTF-8");


								// If last byte is not space, then read 3 more bytes after size. Search for space in it and then trim the number.
								char last = s.charAt(s.length()-1);	
								if(!(last == ' ')){
										byte [] temp = new byte[3];
										file.read(temp);
										for(int i = 0; i < 3; i++){

												// ASCII value 32:space and 10:endOfFile
												if (temp[i] == 32 || temp[i] == 10){
														while(i < 3)
																temp[i++] = 32;
												}
										}
										s = s + new String(temp, "UTF-8");
								}	

								file.close();		

								// Split the string and sort 
								String [] temp_numbers = s.trim().split(" ");

								// Skip the first number if start flag is true	
								int j = 0;
								if (startFlag)
										j++;

								int [] numbers = new int[temp_numbers.length-j];
								for(int i = j; i < temp_numbers.length; i++)
										numbers[i-j] = Integer.parseInt(temp_numbers[i]);
								Arrays.sort(numbers);

								resultFileName	= INTERMEDIATE_DIRECTORY_KEY + fileName + "_" + file_id;
								FileWriter fw = new FileWriter(resultFileName);

								for(int i = 0; i < numbers.length; i++){
										fw.write(numbers[i] + " ");
								}			
								fw.close();
						}
						catch(Exception e){}

						long stopTime = System.currentTimeMillis();
						long elapsedTime = stopTime - startTime;

						System.out.println("Chunk Sorted for " + fileName + ", Offset - " + offset + " and Time taken =" + elapsedTime);
						return(new JobTime(fileName+"_"+file_id, elapsedTime));		
				} 

				@Override
				public boolean ping(){
						return true;
				}

				@Override
				public boolean cleanJob()
				{
						boolean result = true;
						File folder = new File(INTERMEDIATE_DIRECTORY_KEY);
						for(File file:folder.listFiles())
						{
							if(!file.isDirectory())
								result = result & file.delete();
						}
						return result;
				}

				@Override
				public JobTime stopJob(String jobId, int taskId, int replId)
				{
						String key = jobId + String.valueOf(taskId) + String.valueOf(replId);
						String fileName = "";
						String absolutePath = System.getProperty("user.dir");
						if(mergeState.containsKey(key))
						{
								mergeState.put(key, false);
								fileName = mergeFileMap.get(key);
						}
						else if(sortState.containsKey(key)){
								sortState.put(key, false);
								fileName = sortFileMap.get(key);
						}
						JobTime result	= new JobTime(fileName,(long)-1);
						return result;
				}
		// Merge Task
				@Override
				public JobTime doMerge(List<String> files,String file_id)
				{
					try
						{
								float curProbability                = rnd.nextFloat();
								if(curProbability  < this.failProbability)	
								{
										System.out.println("Failing this Task as random number generated is less than fail probability ...");
										return new JobTime("",(long)-1);
								}

								if(1==files.size()) 
									return new JobTime(files.get(0),(long)0);
								long startTime = System.currentTimeMillis();	
								int n = files.size();
								int [] numbers = new int[n];
								Scanner[] fp = new Scanner[n];
								String outFileName = "merge_" + file_id;	
								String absolutePath = System.getProperty("user.dir");

								//System.out.println("Files used in Merge Task");
								//for(int i=0;i<files.size();i++) System.out.print(files.get(i) + " ");
								//System.out.println(" with outFileName " + outFileName);

								try
								{		
										for(int i = 0; i < n; i++)
										{
												fp[i] = new Scanner(new File(absolutePath + "/" +  INTERMEDIATE_DIRECTORY_KEY + files.get(i)));
												if(fp[i].hasNextInt()){
														numbers[i] = fp[i].nextInt();						 
												}
												else{
														numbers[i] = Integer.MAX_VALUE;
												}
										}

										System.out.println("\nStarting Merge task for " + outFileName );
										FileWriter fw = new FileWriter(absolutePath + "/" +  INTERMEDIATE_DIRECTORY_KEY + outFileName);
										PrintWriter pw = new PrintWriter(fw);

										// count is the number of file pointers that are reading the file
										int count = n;
										while(count > 0){
												int min = numbers[0];
												int minFile = 0;

												for(int j = 0; j < n; j++){
														if(min > numbers[j]){
																min = numbers[j];
																minFile = j;
														}					 
												}
												if(min!=Integer.MAX_VALUE) 
														pw.print(String.valueOf(min) + " ");

												if(fp[minFile].hasNextInt()){
														numbers[minFile] = fp[minFile].nextInt();
												}
												else{
														numbers[minFile] = Integer.MAX_VALUE;
														count--;
												}		
										}

										for(int i = 0; i < n; i++){
												fp[i].close();
										}		
										pw.close();
										fw.close();
								}
								catch(IOException e)
								{
									System.out.println("Here "+e);
									return new JobTime("",(long)-1);
								}

								// Stop time
								long stopTime = System.currentTimeMillis();
								long elapsedTime = stopTime - startTime;
								System.out.println("Merge Completed! Time taken = " + elapsedTime);
								return(new JobTime(outFileName, elapsedTime));
						}
						catch(Exception e) 
						{
							System.out.println("Here "+e);
							return new JobTime("",(long)-1);
						}
					}
			}
