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
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ComputeServiceHandler implements ComputeService.Iface{

	private String INPUT_DIRECTORY_KEY = "";
	private String INTERMEDIATE_DIRECTORY_KEY = "";
	private String OUTPUT_DIRECTORY_KEY = "";
	private static String CURRENT_NODE_IP = "";
	private float failProbability = (float)0.0;	
	Random rnd = new Random();
	Map<String, Boolean> sortState;
	Map<String, Boolean> mergeState;
	Map<String, String> sortFileMap;
	Map<String, String> mergeFileMap;

	public ComputeServiceHandler(String INPUT_DIRECTORY_KEY, String INTERMEDIATE_DIRECTORY_KEY, String OUTPUT_DIRECTORY_KEY,float failProbability){
		this.INPUT_DIRECTORY_KEY = INPUT_DIRECTORY_KEY;
		this.INTERMEDIATE_DIRECTORY_KEY = INTERMEDIATE_DIRECTORY_KEY;	
		this.OUTPUT_DIRECTORY_KEY = OUTPUT_DIRECTORY_KEY;	
		this.failProbability = failProbability;

		try{
			CURRENT_NODE_IP = InetAddress.getLocalHost().getHostName();
		}catch(Exception e){
			System.out.println("Unable to get hostname ... ");
		}		
	}



	// Sort Task
	@Override
	public JobTime doSort(String jobId, int taskId, int replId, String fileName, int offset, int count)
	{
		float curProbability = rnd.nextFloat();
		if(curProbability  < this.failProbability)
		{
			System.out.println("Failing this Task as random number generated is less than fail probability ...");
			return new JobTime("",(long)-1);
		}

		System.out.println("\nStarting Sort task for " + fileName);
		long startTime = System.currentTimeMillis();
		/*		
		try
               {
                       Thread.sleep(10000);
               }
               catch(InterruptedException ex){}
		*/
		String resultFileName  = CURRENT_NODE_IP + "_" + fileName + "_" + offset;
		String sortStatusKey = jobId + String.valueOf(taskId) + String.valueOf(replId); 
		sortState.put(sortStatusKey, true);
		sortFileMap.put(sortStatusKey, resultFileName);

		do{
		try{
			// Reading File
			RandomAccessFile file = new RandomAccessFile(INPUT_DIRECTORY_KEY + fileName, "r");
			if(offset > 0)
				file.seek(offset-1);		
			else
				file.seek(offset);		
	
			// Read offset-1 and offset byte from file. If space is present in either then skip the first number		
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
			for(int i = j; i < temp_numbers.length; i++){
				numbers[i-j] = Integer.parseInt(temp_numbers[i]);
				if(!sortState.get(sortStatusKey))
					break;
			}
			
			if(!sortState.get(sortStatusKey))
				break;

			Arrays.sort(numbers);

//			resultFileName	= INTERMEDIATE_DIRECTORY_KEY + CURRENT_NODE_IP + "_" + fileName + "_" + offset;
			FileWriter fw = new FileWriter(INTERMEDIATE_DIRECTORY_KEY + resultFileName);
			
			for(int i = 0; i < numbers.length; i++){
				fw.write(numbers[i] + " ");
                                if(!sortState.get(sortStatusKey))
                                        break;				
			}			
			fw.close();
//			sortState[sortStatusKey] = false;
		}
		catch(Exception e){
//			sortState[sortStatusKey] = false;
			System.out.println("Error in writing to file");
		}
		}while(false);

		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;


		if(!sortState.get(sortStatusKey))
                return(new JobTime("", elapsedTime));			

		System.out.println("Chunk Sorted for " + fileName + ", Offset - " + offset + " and Time taken =" + elapsedTime);
		return(new JobTime(resultFileName, elapsedTime));		
	} 
	
	@Override
	public boolean ping(){
		return true;
	}


	// Merge Task
	@Override
	public JobTime doMerge(String jobId, int taskId, int replId, List<String> files){

	        float curProbability = rnd.nextFloat();
		if(curProbability  < this.failProbability)	
		{
			System.out.println("Failing this Task as random number generated is less than fail probability ...");
			return new JobTime("",(long)-1);
		}

		long startTime = System.currentTimeMillis();	
		int n = files.size();
		int [] numbers = new int[n];
		Scanner[] fp = new Scanner[n];
		String outFileName = CURRENT_NODE_IP + "_merge_" + Util.hashFile(files);	
		String absolutePath = System.getProperty("user.dir");
                String mergeStatusKey = jobId + String.valueOf(taskId) + String.valueOf(replId);
                mergeState.put(mergeStatusKey, true);
		mergeFileMap.put(mergeStatusKey, outFileName);

		/*
		try
               {
                       Thread.sleep(30000);
               }
               catch(InterruptedException ex){}
		*/
		do{
		try{		
			for(int i = 0; i < n; i++){
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
				
				// Check for proactiveness
				if(!mergeState.get(mergeStatusKey))
					break;
	
				for(int j = 0; j < n; j++){
					if(min > numbers[j]){
						min = numbers[j];
						minFile = j;
					}					 
				}
				if(min != Integer.MAX_VALUE) 
					pw.print(String.valueOf(min) + " ");
				
				if(fp[minFile].hasNextInt()){
					numbers[minFile] = fp[minFile].nextInt();
				}
				else{
					numbers[minFile] = Integer.MAX_VALUE;
					count--;
				}		
			}
			
			// Check for proactiveness
			if(!mergeState.get(mergeStatusKey))
                        	break;

			for(int i = 0; i < n; i++){
				fp[i].close();
			}		
			pw.close();
			fw.close();
	
			// Deleting intermediate sort/merge files
			for(int i = 0; i < files.size(); i++){
				File f = new File(absolutePath + "/" + INTERMEDIATE_DIRECTORY_KEY + files.get(i));
				f.delete();

				// Check for proactiveness
                                if(!mergeState.get(mergeStatusKey))
                                        break;
			}			
		}
		catch(IOException e){
			System.out.println("Something wrong with Input");
			e.printStackTrace();
		}
		}while(false);

		// Stop time
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;

		if(!mergeState.get(mergeStatusKey))
			return(new JobTime("", elapsedTime));

		System.out.println("Merge Completed! Time taken = " + elapsedTime);
		return(new JobTime(outFileName, elapsedTime));
	}


	public boolean updateStatus(String jobId, int taskId, int replId, int type, boolean shouldStop){
		String key = jobId + String.valueOf(taskId) + String.valueOf(replId);
		String fileName = "";
		if(type == 0){
			sortState.put(key, false);
			fileName = sortFileMap.get(key);
		}
		else{
			mergeState.put(key, false);
			fileName = mergeFileMap.get(key);
		}
		
                String absolutePath = System.getProperty("user.dir");
		File file = new File(absolutePath + INTERMEDIATE_DIRECTORY_KEY + fileName);
		file.delete();
		return true;
	}

	@Override
	public JobTime stopJob(String jobId, int taskId, int replId)
	{
		JobTime result	= new JobTime("",(long)0);
		return result;
	}

	@Override
	public JobTime completeJob(Map<JobTime,Boolean> action)
	{
		JobTime result	= new JobTime("",(long)0);
		return result;
	}
	
	@Override
	public void cleanJob(String key){
		if(mergeState.containsKey(key))
			mergeState.remove(key);
		if(mergeFileMap.containsKey(key))
			mergeFileMap.remove(key);
		if(sortState.containsKey(key))
			sortState.remove(key);
		if(sortFileMap.containsKey(key))
			sortFileMap.remove(key);			
	}


}
