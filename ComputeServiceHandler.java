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


	@Override
	public JobTime doSort(String fileName, int offset, int count){

		System.out.println("\nStarting Sort task for " + fileName);
		long startTime = System.currentTimeMillis();
		List<Integer> items = new ArrayList<>();

		try
		{
			// Read input from file
			for(String line : Files.readAllLines(Paths.get(INPUT_DIRECTORY_KEY + fileName),Charset.forName("US-ASCII"))){
					for(String part : line.split("\n")){
					items.add(Integer.parseInt(part));
				}
			}

			Collections.sort(items);
	
			// Write output to file		
			FileWriter fw = new FileWriter(INTERMEDIATE_DIRECTORY_KEY + fileName);
			for(int i = 0 ; i < items.size(); i++)
				fw.write(String.valueOf(items.get(i)) + "\n");
		}
		catch(IOException e){}
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;

		System.out.println("Chunk Sorted for " + fileName + ", Offset - " + offset + " and Time taken =" + elapsedTime);
		return(new JobTime(fileName, elapsedTime));		
	} 
	

	// Add two more parameters
	//	1) Chunk no
	//	2) Intermediate Merge or Final Merge
	@Override
	public JobTime doMerge(List<String> files){

		long startTime = System.currentTimeMillis();	
		int n = files.size();
		int [] numbers = new int[n];
		BufferedReader[] fp = new BufferedReader[n];
		
		String outFileName = "m";
		try{		
		for(int i = 0; i < n; i++){
//			String fileName = files[i];
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
