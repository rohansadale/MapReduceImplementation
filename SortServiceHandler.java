import java.util.*;
import java.lang.System;
import java.lang.Runnable;
import java.util.concurrent.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;

public class SortServiceHandler implements SortService.Iface
{
	List<Node> computeNodes;
	private static int chunkSize				= 0;
	private static int mergeTaskSize			= 0;
	private static String inputDirectory		= "";
	private static String intermediateDirectory	= "";
	private static String outputDirectory		= "";	

	public SortServiceHandler(Node node,String inputDirectory,String intermediateDirectory,String outputDirectory,
							int chunkSize,int mergeTaskSize)
	{
		computeNodes				= new ArrayList<Node>();
		this.chunkSize				= chunkSize;
		this.mergeTaskSize			= mergeTaskSize;
		this.inputDirectory			= inputDirectory;
		this.intermediateDirectory	= intermediateDirectory;
		this.outputDirectory		= outputDirectory;
	}
	
	@Override
	public JobStatus doJob(String filename) throws TException
	{
		/*
		JobStatus is structure
			- status  	=> true/false whether task succesfully finished or not
			- filename	=> if successful then return name of the file where sorted output is stored
			- message	=> if task failed, then it contains message why task failed
		*/	
		System.out.println("Request received for sorting file " + filename);
		JobStatus result = new JobStatus(false,"","");
		if(0==computeNodes.size())
		{
			result.message	= "No compute Node available";
			return result;
		}
	
		File sortFile		= new File(inputDirectory+filename);
		if(!sortFile.exists())
		{
			result.message	= "File " + filename + " not present";
			return result;
		}		
	
		double fileSize		= sortFile.length();
		int numTasks		= Math.ceil(fileSize/5*chunkSize); 
		System.out.println("To sort " + filename + " " + numTasks + " are produced");	
				

	}
	
	@Override
	public String join(Node node) throws TException
	{
		if(null==node) return false;
		computeNodes.add(node);
		return true;
	}

	static class sortJob extends Thread
	{
		public String filename;
		public int offSet;
		public int numToSort;
		public long duration;		
		public String ip;
		public int port;
	
		public sortJob(String filename,int offSet,int numToSort,String ip,int port)
		{
			this.filename	= filename;
			this.offSet		= offSet;
			this.numToSort	= numToSort;
			this.duration	= 0;
			this.ip			= ip;
			this.port		= port;	
		}

		public void run()
		{
			
		}
	}
}
