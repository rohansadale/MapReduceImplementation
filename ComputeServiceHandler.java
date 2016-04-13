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

	public ComputeServiceHandler(String INPUT_DIRECTORY_KEY, String INTERMEDIATE_DIRECTORY_KEY, String OUTPUT_DIRECTORY_KEY){
		this.INPUT_DIRECTORY_KEY = INPUT_DIRECTORY_KEY;
		this.INTERMEDIATE_DIRECTORY_KEY = INTERMEDIATE_DIRECTORY_KEY;	
		this.OUTPUT_DIRECTORY_KEY = OUTPUT_DIRECTORY_KEY;	
	}


	// Sort Task
	@Override
	public JobTime doSort(String fileName, int offset, int count){

		System.out.println("\nStarting Sort task for " + fileName);
		long startTime = System.currentTimeMillis();

		String resultFileName	 = "";
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
		
			int [] numbers = new int[temp_numbers.length];
	
			for(int i = 0; i < numbers.length; i++)
				numbers[i] = Integer.parseInt(temp_numbers[i]);
		
			Arrays.sort(numbers);

			resultFileName	= INTERMEDIATE_DIRECTORY_KEY + fileName + "_" + offset;
			FileWriter fw = new FileWriter(resultFileName);

			// Skip the first number if start flag is true	
			int j = 0;
			if (startFlag)
				j++;
			
			for(int i = j; i < numbers.length; i++){
				fw.write(numbers[i] + "\n");
			}			
			fw.close();
		}
		catch(Exception e){
			System.out.println("Error in writing to file");
		}

		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;

		System.out.println("Chunk Sorted for " + fileName + ", Offset - " + offset + " and Time taken =" + elapsedTime);
		return(new JobTime(fileName+"_"+offset, elapsedTime));		
	} 
	

	// Merge Task
	@Override
	public JobTime doMerge(List<String> files){

		long startTime = System.currentTimeMillis();	
		int n = files.size();
		int [] numbers = new int[n];
		BufferedReader[] fp = new BufferedReader[n];
		String outFileName = "merge_" + Util.hashFile(files);	

		try{		
			for(int i = 0; i < n; i++){
				fp[i] = new BufferedReader(new FileReader(INTERMEDIATE_DIRECTORY_KEY + files.get(i)));
				String no = fp[i].readLine();
				if(no != null)
					numbers[i] = Integer.parseInt(no);
				else
					numbers[i] = Integer.MAX_VALUE; 
			}
	
			System.out.println("\nStarting Merge task for " + outFileName );
			FileWriter fw = new FileWriter(outFileName);
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
				pw.println(min);
				String no = fp[minFile].readLine();
				if(no != null){
					numbers[minFile] = Integer.parseInt(no);
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
	
			// Deleting intermediate sort/merge files
			for(int i = 0; i < files.size(); i++){
				File f = new File(INTERMEDIATE_DIRECTORY_KEY + files.get(i));
				f.delete();
			}
	
		}
		catch(IOException e){
			System.out.println("Something wrong with Input");
			e.printStackTrace();
		}

		// Stop time
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		System.out.println("Merge Completed! Time taken = " + elapsedTime);
		return(new JobTime(outFileName, elapsedTime));
	}


	@Override
	public boolean ping(){
		return true;
	}

}
