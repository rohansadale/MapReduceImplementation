import java.io.*;
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
	private static int syncInterval				= 30000;
	private static ArrayList<sortJob> jobs;

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
		jobs						= new ArrayList<sortJob>();
	}
	
	public void healthCheck()
	{
		Runnable syncThread = new Runnable()
		{
         	public void run()
			{
				while(true)
				{
        			try
					{
						Thread.sleep(syncInterval);
						for(int i=0;i<computeNodes.size();)
						{
							boolean isAlive = isNodeAlive(computeNodes.get(i).ip,computeNodes.get(i).port);
							if(!isAlive) reAssign(i);
							else i++;
						}
					}
					catch(InterruptedException ex) {}
				}
			}
     	};
		new Thread(syncThread).start();
	}

	@Override
	public boolean join(Node node) throws TException
	{
		if(null==node) return false;
		computeNodes.add(node);
		return true;
	}
	
	/*
	JobStatus is structure
		- status  	=> true/false whether task succesfully finished or not
		- filename	=> if successful then return name of the file where sorted output is stored
		- message	=> if task failed, then it contains message why task failed
	*/	
	@Override
	public JobStatus doJob(String filename) throws TException
	{
		System.out.println("Request received for sorting file " + filename);
		jobs.clear();
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
		int numTasks		= (int)Math.ceil(fileSize/chunkSize); 
		System.out.println("To sort " + filename + " " + numTasks + " are produced");	
		
		int offset					= 0;
		for(int i=0;i<numTasks;i++)
		{
			Collections.shuffle(computeNodes);
			for(int j=0;j<replication;j++)
			{
				sortJob job				= new sortJob(filename,offset,chunkSize,computeNodes.get(j).ip,computeNodes.get(j).port);
				jobs.add(job);	
			}
			offset		= offset + chunkSize;
		}	

		return sortAndMerge(jobs);
	}
	
	private JobStatus sortAndMerge(ArrayList< sortJob > jobs) throws TException
	{		
		try
		{
			for(int i=0;i<jobs.size();i++)
					jobs.get(i).start();
			for(int i=0;i<jobs.size();i++)
					jobs.get(i).join();
		}
		catch(InterruptedException e) {}
	
		List<String> intermediateFiles		= new ArrayList<String>();
		for(int i=0;i<jobs.size();i++)
		{
			intermediateFiles.add(jobs.get(i).destFileName);
			System.out.println("Processing of Task on node " + jobs.get(i).ip + " finished in " + jobs.get(i).duration + " milli-seconds");
		}

		return mergeFiles(intermediateFiles);
	}
	
	private JobStatus mergeFiles(List<String> intermediateFiles) throws TException
	{
		JobStatus result = new JobStatus(false,"","");
		System.out.println("Starting Merging files ..... ");
		while(intermediateFiles.size()!=1)
		{
			List<String> tFiles				= new ArrayList<String>();
			for(int i=0;i<Math.min(mergeTaskSize,intermediateFiles.size());i++)
				tFiles.add(intermediateFiles.get(i));
			intermediateFiles.add(merge(tFiles));
		}
		
		result.status	= true;
		result.filename	= intermediateFiles.get(0);
		System.out.println("Sorted output is stored in " + result.filename);
		String absolutePath	= System.getProperty("user.dir");
		new File(absolutePath+ "/" + intermediateDirectory + result.filename).renameTo(new File(absolutePath +"/" + outputDirectory + result.filename));
		return result;
	}
	

	private String merge(List<String> intermediateFiles) throws TException
	{
		Collections.shuffle(computeNodes);
		String ip		= computeNodes.get(0).ip;
		int port		= computeNodes.get(0).port;
		String result	= "";

		try
        {
          	TTransport transport                = new TSocket(ip,port);
            TProtocol protocol                  = new TBinaryProtocol(new TFramedTransport(transport));
            ComputeService.Client client        = new ComputeService.Client(protocol);
            transport.open();
            result			                    = client.doMerge(intermediateFiles);
            transport.close();
       }
       catch(TException x)
       {
			System.out.println(" =================== Unable to establish connection with Node " + ip + "... Exiting ... =================");
       }	
	   return result;
	}	

	void reAssign(int idx)
	{
		int seed = (int)((long)System.currentTimeMillis() % 1000);
		Random rnd = new Random(seed);

		String requiredIP	= computeNodes.get(idx).ip;
		System.out.println("Node with IP " + computeNodes.get(idx).ip + " failed !!!!!!!! Re-assigning its Task ...");
		computeNodes.remove(idx);
	
		int i = 0;
		while(i<jobs.size())
		{
			if(jobs.get(i).ip.equals(requiredIP)==true)
			{
				jobs.add(new sortJob(jobs.get(i).filename,jobs.get(i).offSet,jobs.get(i).numToSort,
						computeNodes.get(rnd.nextInt(computeNodes.size())).ip,computeNodes.get(rnd.nextInt(computeNodes.size())).port));
				jobs.remove(i);
			}
		}	
	}

	boolean isNodeAlive(String ip,int port)
	{
		boolean status	= false;
		try
		{
			TTransport transport				= new TSocket(ip,port);
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			ComputeService.Client client		= new ComputeService.Client(protocol);
			transport.open();
			status								= client.ping();
			transport.close();
		}

		catch(TException x)
		{
			System.out.println(" =================== Unable to establish connection with Node " + ip + "... Exiting ... =================");
		}	
		return false;
	}

	static class sortJob extends Thread
	{
		public String filename;
		public int offSet;
		public int numToSort;
		public long duration;		
		public String ip;
		public int port;
		public String destFileName;	

		public sortJob(String filename,int offSet,int numToSort,String ip,int port)
		{
			this.filename		= filename;
			this.offSet			= offSet;
			this.numToSort		= numToSort;
			this.duration		= 0;
			this.ip				= ip;
			this.port			= port;	
			this.destFileName	= "";
		}

		public void run()
		{
			try
			{
				TTransport transport				= new TSocket(ip,port);
				TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
				ComputeService.Client client		= new ComputeService.Client(protocol);
				transport.open();
				this.destFileName					= client.doSort(filename,offSet,numToSort);
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
