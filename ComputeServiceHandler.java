import java.util.*;
import java.lang.System;
import java.lang.Runnable;
import java.util.concurrent.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.thrift.protocol.TBinaryProtocol;

public class ComputeServiceHandler implements Compute.Iface{

	private String INPUT_DIRECTORY_KEY = "";
	private String INTERMEDIATE_DIRECTORY_KEY = "";
	private String OUTPUT_DIRECTORY_KEY = "";

	public ComputeServiceHandler(Node currentNode, String INPUT_DIRECTORY_KEY, String INTERMEDIATE_DIRECTORY_KEY, String OUTPUT_DIRECTORY_KEY){
		this.INPUT_DIRECTORY_KEY = INPUT_DIRECTORY_KEY;
		this.INTERMEDIATE_DIRECTORY_KEY = INTERMEDIATE_DIRECTORY_KEY;	
		this.OUTPUT_DIRECTORY_KEY = OUTPUT_DIRECTORY_KEY;	
	}



	@Override
	public Time doSort(String fileName, int offset, int count){
		long startTime = System.currentTimeMillis();
		List<Integer> items = new ArrayList<>();

		// Read input from file
		for(String line : Files.readAllLines(Paths.get(INPUT_DIRECTORY_KEY + fileName))){
			for(String part : line.split("\n")){
				items.add(Integer.parseInt(part));
			}
		}

		Arrays.sort(items);

		// Write output to file		
		FileWriter fw = new FileWriter(INTERMEDIATE_DIRECTORY_KEY + fileName);
		for(int i = 0 ; i < items.length(); i++)
			fw.write(items[i] + "\n");
		
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;

		return(new Time(filename, elapsedTime));		
	} 
	
	@Override
	public Time doMerge(List<String> files){
		
	}

	@Override
	public boolean ping(){
		return true;
	}
		
}
