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
	private static int replication				= 0;

	public SortServiceHandler(Node node,String inputDirectory,String intermediateDirectory,String outputDirectory,
							int chunkSize,int mergeTaskSize,int replication)
	{
		computeNodes				= new ArrayList<Node>();
		this.chunkSize				= chunkSize;
		this.mergeTaskSize			= mergeTaskSize;
		this.inputDirectory			= inputDirectory;
		this.intermediateDirectory	= intermediateDirectory;
		this.outputDirectory		= outputDirectory;
		this.replication			= replication;
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
		int numTasks		= Math.ceil(fileSize/chunkSize); 
		System.out.println("To sort " + filename + " " + numTasks + " are produced");	
		
		ArrayList< ArrayList<sortJob> > jobs		= new ArrayList< ArrayList<sortJob> >();
		int offset					= 0;
		for(int i=0;i<numTasks;i++)
		{
			Collections.shuffle(computeNodes);
			ArrayList<sortJob> chunkJob				= new ArrayList<sortJob>();
			for(int j=0;j<replication;j++)
			{
				sortJob job				= new sortJob(filename,offset,chunkSize,0,computeNodes.get(j).ip,computeNodes.get(j).port);
				chunkJob.add(job);	
			}
			jobs.add(chunkJob);
			offset		= offset + chunkSize;
		}	
		
		try
		{
			for(int i=0;i<jobs.size();i++)
			{
				for(int j=0;j<jobs.get(i).size();j++)
					jobs.get(i).get(j).start();
			}
			for(int i=0;i<jobs.size();i++)
			{
				for(int j=0;j<jobs.get(i).size();j++)
					jobs.get(i).get(j).join();
			}
		}
		catch(InterruptedException e) {}
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
			try
			{
				TTransport transport				= new TSocket(ip,port);
				TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
				QuorumService.Client client			= new QuorumService.Client(protocol);
				transport.open();
				startNode							= client.GetNode();
				transport.close();
			}

			catch(TException x)
			{
				System.out.println(" =================== Unable to establish connection with Node " + ip + "... Exiting ... =================");
				return;
			}	
		}
	}
}
