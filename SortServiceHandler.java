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
	private static int healthCheckInterval		= 10000;
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
						Thread.sleep(healthCheckInterval);
						for(int i=0;i<computeNodes.size();)
						{
							boolean isAlive = isNodeAlive(computeNodes.get(i).ip,computeNodes.get(i).port);
							if(!isAlive) 
							{
								System.out.println("Removing " + computeNodes.get(i).ip + " from the system");
								removeNode(i);
							}
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
		Util.getInstance().printNodeList(computeNodes);
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
		System.out.println("To sort " + filename + " " + numTasks + " Tasks are produced");	
		
		int offset					= 0;
		for(int i=0;i<numTasks;i++)
		{
			Collections.shuffle(computeNodes);
			for(int j=0;j<replication;j++)
			{
				sortJob job				= new sortJob(i,filename,offset,chunkSize,computeNodes.get(j).ip,computeNodes.get(j).port);
				jobs.add(job);	
			}
			offset		= offset + chunkSize;
		}	

		return sortAndMerge(jobs);
	}
	
	private JobStatus sortAndMerge(ArrayList< sortJob > jobs) throws TException
	{
		for(int i=0;i<jobs.size();i++) 
		{
			System.out.println("Starting Job with id " + jobs.get(i).id + " on node " + jobs.get(i).ip);
			jobs.get(i).start();
		}

		while(true)
		{
			int finishedJobs	= 0;
			for(int i=0;i<jobs.size();i++)
			{
				if(1==jobs.get(i).threadRunStatus) finishedJobs	= finishedJobs+1;
				if(2==jobs.get(i).threadRunStatus) 
				{
					sortJob retry	= reAssign(jobs.get(i));
					jobs.remove(i);
					jobs.add(retry);
					retry.start();
				}
			}
			if(finishedJobs==jobs.size()) break;
		}	
		
		System.out.println("Sorting Task finished!!");	
		List<String> intermediateFiles		= new ArrayList<String>();
		for(int i=0;i<jobs.size();i++)
		{
			intermediateFiles.add(jobs.get(i).result.filename);
			System.out.println("Processing of Task on node " + jobs.get(i).ip + " finished in " + jobs.get(i).result.time + " milli-seconds");
		}

		return mergeFiles(intermediateFiles);
	}
	
	private JobStatus mergeFiles(List<String> intermediateFiles) throws TException
	{
		JobStatus result 					= new JobStatus(false,"","");
		System.out.println("Starting Merging files ..... ");
		Queue<String> q1					= new LinkedList<String>();
		Queue<String> q2					= new LinkedList<String>();
		JobTime mergeJobStatus				= null;
		int seed 							= (int)((long)System.currentTimeMillis() % 1000);
        Random rnd 							= new Random(seed);
		
		for(int i=0;i<intermediateFiles.size();i++)  q1.add(intermediateFiles.get(i));
		
		while(!q1.isEmpty())
		{
			if(q1.size()==1) break;
			while(!q1.isEmpty())
			{
				List<String> tFiles             = new ArrayList<String>();
				int initQueueSize				= q1.size();
				for(int i=0;i<Math.min(mergeTaskSize,initQueueSize);i++)
				{
					tFiles.add(q1.peek());
					q1.remove();
				}
				do
				{
					int merge_idx  		= rnd.nextInt(computeNodes.size());
					System.out.println("Merge Request sent to node " + computeNodes.get(merge_idx).ip);
					mergeJobStatus		= merge(tFiles,computeNodes.get(merge_idx).ip,computeNodes.get(merge_idx).port);
					if(null==mergeJobStatus) 
					{
						System.out.println("Merge Request sent to node " + computeNodes.get(merge_idx).ip + "  Failed");
						computeNodes.remove(merge_idx);
					}
				}while(mergeJobStatus==null);
				q2.add(mergeJobStatus.filename);
			}
			while(!q2.isEmpty())
			{
				q1.add(q2.peek());
				q2.remove();
			}
		}	
		
		result.status	= true;
		result.filename	= q1.peek();
		System.out.println("Sorted output is stored in " + result.filename);
		String absolutePath	= System.getProperty("user.dir");
		processFile(absolutePath + "/" + intermediateDirectory + result.filename);
		new File(absolutePath+ "/" + intermediateDirectory + result.filename).renameTo(new File(absolutePath +"/" + outputDirectory + result.filename));
		return result;
	}
	

	private void processFile(String filename)
	{
		try
		{
			BufferedReader reader					= new BufferedReader(new FileReader(filename));
			String number							= "";
			ArrayList<String> content				= new ArrayList<String>();
			while((number = reader.readLine())!=null) content.add(number);
			reader.close();
	
			PrintWriter pw							= new PrintWriter(new FileWriter(filename));
			for(int i=0;i<content.size();i++)
			{
				pw.print(content.get(i));
				if(i!=content.size()-1) pw.print(" ");
			}
			pw.close();
		}
		catch(IOException e)
		{
			System.out.println("Error occured while converting output file to space delimited " + e);
		}
	}

	private JobTime merge(List<String> intermediateFiles,String ip,int port) throws TException
	{
		JobTime result	= null;

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
			System.out.println(" =================== Unable to establish connection with Node " + ip + " Merge Job Failed ... Exiting ... =================");
       }	
	   return result;
	}	

	void removeNode(int idx)
	{
		System.out.println("Node with IP " + computeNodes.get(idx).ip + " failed !!!!!!!! Re-assigning its Task ...");
		computeNodes.remove(idx);
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
			System.out.println(" =================== Unable to establish connection with Node " + ip + " - Status Check failed ... Exiting ... =================");
		}	
		return status;
	}

	sortJob reAssign(sortJob job)
	{
		int seed 			= (int)((long)System.currentTimeMillis() % 1000);
        Random rnd 			= new Random(seed);
		int retry_task_idx  = rnd.nextInt(computeNodes.size());
		System.out.println("Task with Id " + job.id + " will be re-assigned to node with IP " + computeNodes.get(retry_task_idx).ip);	
		sortJob retry		= new sortJob(job.id,job.filename,job.offSet,job.numToSort,computeNodes.get(retry_task_idx).ip,computeNodes.get(retry_task_idx).port);
		return retry;
	}

	static class sortJob extends Thread
	{
		public String filename;
		public int offSet;
		public int numToSort;
		public long duration;		
		public String ip;
		public int port;
		public JobTime result;
		public int threadRunStatus;
		public int id;

		public sortJob(int id,String filename,int offSet,int numToSort,String ip,int port)
		{
			this.id					= id;
			this.filename			= filename;
			this.offSet				= offSet;
			this.numToSort			= numToSort;
			this.duration			= 0;
			this.ip					= ip;
			this.port				= port;	
			this.result				= null;
			this.threadRunStatus	= 0;
		}

		public void run()
		{
			try
			{
				TTransport transport				= new TSocket(ip,port);
				TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
				ComputeService.Client client		= new ComputeService.Client(protocol);
				transport.open();
				this.result							= client.doSort(filename,offSet,numToSort);
				transport.close();
				this.threadRunStatus				= 1;
			}

			catch(TException x)
			{
				System.out.println(" =================== Unable to establish connection with Node " + ip + " Sort Job Failed - Exiting ... =================");
				this.threadRunStatus				= 2;
			}	
		}
	}
}
