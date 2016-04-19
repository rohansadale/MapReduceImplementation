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
	private static int chunkSize						= 0;
	private static int mergeTaskSize					= 0;
	private static String inputDirectory				= "";
	private static String intermediateDirectory			= "";
	private static String outputDirectory				= "";	
	private static int replication						= 0;
	private static int healthCheckInterval				= 10000;
	private static int sortFailedJobs					= 0;
	private static int mergeFailedJobs					= 0;
	private static int mergeJobs						= 0;
	private static HashMap<String,Long>	mergeDuration	= null;
	private static HashMap<String,Integer> mergeCount	= null;
	private static int initSystemSize					= 0;
	private static int threadPoolCount 					= 50;
	private static ArrayList< ArrayList<sortJob> >  jobs;
	private static ArrayList< ArrayList<mergeJob> > mjobs;

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
		this.sortFailedJobs			= 0;
		this.mergeFailedJobs		= 0;
		this.mergeJobs				= 0;
		this.initSystemSize			= 0;
		jobs						= new ArrayList< ArrayList<sortJob> >();
		mjobs						= new ArrayList< ArrayList<mergeJob> >();
		mergeDuration				= new HashMap<String,Long>();
		mergeCount					= new HashMap<String,Integer>();
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

	private void clearVariables()
	{
		for(int i=0;i<jobs.size();i++) jobs.get(i).clear();
		for(int i=0;i<mjobs.size();i++) mjobs.get(i).clear();
		jobs.clear();
		mjobs.clear();
		sortFailedJobs 		= 0;
		mergeFailedJobs		= 0;
		mergeJobs			= 0;	
		initSystemSize		= computeNodes.size();	
		mergeDuration		= new HashMap<String,Long>();
		mergeCount			= new HashMap<String,Integer>();
	}
	
	@Override
	public boolean join(Node node) throws TException
	{
		if(null==node) return false;
		computeNodes.add(node);
		Util.getInstance().printNodeList(computeNodes);
		return true;
	}
	
	private ArrayList<sortJob> processSortJobs(ArrayList< ArrayList< sortJob > > jobs) throws TException
	{
		int finishedJobs 				= 0;
		int failedReplicatedJobs		= 0;
		ArrayList<JobTime> killedJobs	= null;
		ArrayList<sortJob> success 		= new ArrayList<sortJob>();
		int [] hasProcessed 			= new int[jobs.size()];
		boolean completeJobStatus		= false;
		ExecutorService executor 		= Executors.newFixedThreadPool(threadPoolCount);
		for(int i=0;i<jobs.size();i++)
		{
			for(int j=0;j<jobs.get(i);j++)
				executor.execute(jobs.get(i).get(j));
		}

		while(true)
		{
			for(int i=0;i<jobs.size();i++)
			{
				if(1==hasProcessed[i]) 
				{
					finishedJobs			= finishedJobs+1;
					continue;
				}
				failedReplicatedJobs 			= 0;
				for(int j=0;j<jobs.get(i).size();j++)
				{
					if(1==jobs.get(i).get(j).threadRunStatus)
					{
						killedJobs				= Util.getInstance().stopSortJob(jobs.get(i),j);
						finishedJobs 			= finishedJobs+1;
						sortFailedJobs			= sortFailedJobs+killedJobs.size();
						killedJobs.add(jobs.get(i).get(j).result);
						success.add(jobs.get(i).get(j));
						hasProcessed[i]				= 1;
						break;
					}
					if(2==jobs.get(i).get(j).threadRunStatus)
						failedReplicatedJobs	= failedReplicatedJobs+1;
				}
				if(failedReplicatedJobs==jobs.get(i).size()) 
				{
					success.clear();
					executor.shutdown();
					return success;
				}
			}
			if(finishedJobs==jobs.size()) break;
			finishedJobs	= 0;
		}
		executor.shutdown();
		return success;
	}

	private ArrayList< sortJob > doSort(String sortFile) throws TException
	{
		File filename				= new File(inputDirectory+sortFile);
		double fileSize				= filename.length();
		int numTasks				= Math.max(1,(int)Math.ceil(fileSize/chunkSize));
		System.out.println("To sort " + sortFile + " " + numTasks + " Tasks are produced");

		int offset					= 0;
		int iter 					= 0;
		for(int i=0;i<numTasks;i++)
		{
			Collections.shuffle(computeNodes);
			ArrayList<sortJob> replSortJob	= new ArrayList<sortJob>();
			for(int j=0;j<replication;j++)
			{
				sortJob job					= new sortJob(jobId,iter,j,sortFile,offset,
													chunkSize,computeNodes.get(j).ip,computeNodes.get(j).port);
				replSortJob.add(job);
				iter 						= iter+1;
			}
			jobs.add(replSortJob);
			offset		= offset + chunkSize;
		}
		return processSortJobs(jobs);
	}

	private ArrayList<mergeJob> processMergeJobs(ArrayList< ArrayList< mergeJob > > jobs) throws TException
	{
		int finishedJobs 					= 0;
		int failedReplicatedJobs			= 0;
		ArrayList<JobTime> killedJobs		= null;
		ArrayList<mergeJob> success 		= new ArrayList<mergeJob>();
		int [] hasProcessed 				= new int[jobs.size()];
		boolean completeJobStatus			= false;
		while(true)
		{
			for(int i=0;i<jobs.size();i++)
			{
				if(1==hasProcessed[i]) 
				{
					finishedJobs			= finishedJobs+1;
					continue;
				}
				failedReplicatedJobs 			= 0;
				for(int j=0;j<jobs.get(i).size();j++)
				{
					if(1==jobs.get(i).get(j).threadRunStatus)
					{
						killedJobs			= Util.getInstance().stopMergeJob(jobs.get(i),j);
						finishedJobs 		= finishedJobs+1;
						mergeFailedJobs 	= mergeFailedJobs+killedJobs.size();
						killedJobs.add(jobs.get(i).get(j).result);
						success.add(jobs.get(i).get(j));
						hasProcessed[i]		= 1;
						break;
					}
					if(2==jobs.get(i).get(j).threadRunStatus)
						failedReplicatedJobs	= failedReplicatedJobs+1;
				}
				if(failedReplicatedJobs==jobs.get(i).size()) 
				{
					success.clear();
					return success;
				}
			}
			if(finishedJobs==jobs.size()) break;
			finishedJobs	= 0;
		}
		return success;
	}

	private JobStatus doMerge(ArrayList< sortJob > sortResult) throws TException
	{
		JobStatus result 					= new JobStatus(false,"","");
		System.out.println("Starting Merging files ..... ");
		ArrayList<String> intermediateFiles = new ArrayList<String>();
		for(int i=0;i<sortResult.size();i++)
			intermediateFiles.add(sortResult.get(i).result.filename);
		int seed 							= (int)((long)System.currentTimeMillis() % 1000);
        Random rnd 							= new Random(seed);
		mergeJobs							= 0;
		int iter 							= replication*jobs.size()+1;

		while(true)
		{
			mjobs.clear();
			int finishedJobs				= 0;
			int task_node_idx				= 0;
			System.out.println("Started Fresh Round of Merging  with " + intermediateFiles.size() + " files");
			ExecutorService executor 		= Executors.newFixedThreadPool(threadPoolCount/5);
			for(int i=0;i<intermediateFiles.size();i=i+mergeTaskSize)
			{
				List<String> tFiles				= new ArrayList<String>();
				for(int j=i;j<i+mergeTaskSize && j<intermediateFiles.size();j++)
					tFiles.add(intermediateFiles.get(j));

				Collections.shuffle(computeNodes);
				ArrayList<mergeJob> replSortJob	= new ArrayList<mergeJob>();
				
				for(int j=0;j<replication;j++)
				{
					mergeJob cmergeJob			= new mergeJob(jobId,iter,j,tFiles,
														computeNodes.get(j).ip,computeNodes.get(j).port);
					replSortJob.add(cmergeJob);
					iter 						= iter+1;
				}
				
				mjobs.add(replSortJob);
			}
			
			for(int i=0;i<mjobs.size();i++)
			{	
				for(int j=0;j<mjobs.get(i).size();j++)
					executor.execute(mjobs.get(i).get(j));
				mergeJobs							= mergeJobs + mjobs.get(i).size();
			}
			ArrayList<mergeJob> success 		= processMergeJobs(mjobs);
			executor.shutdown();
			intermediateFiles.clear();
			for(int i=0;i<success.size();i++)
			{
					if(mergeCount.containsKey(success.get(i).ip)==false)
					{
							mergeCount.put(success.get(i).ip,0);
							mergeDuration.put(success.get(i).ip,(long)0L);
					}
					mergeCount.put(success.get(i).ip,mergeCount.get(success.get(i).ip)+1);
					mergeDuration.put(success.get(i).ip,mergeDuration.get(success.get(i).ip) + success.get(i).result.time);
					intermediateFiles.add(success.get(i).result.filename);
			}
			if(intermediateFiles.size()==1) break;
		}

		result.status	= true;
		result.filename	= intermediateFiles.get(0);
		System.out.println("Sorted output is stored in " + result.filename);
		String absolutePath	= System.getProperty("user.dir");
		new File(absolutePath+ "/" + intermediateDirectory + result.filename).renameTo(new File(absolutePath +"/" + outputDirectory + result.filename));
		return result;
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
		clearVariables();

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
		
		ArrayList< sortJob > sortResult		= doSort(filename);
		if(sortResult.isEmpty()==true) 
		{
			Util.getInstance().cleanIntermediateFiles(computeNodes.get(0).ip,computeNodes.get(0).port);
			return new JobStatus(false,"Sort Job Failed as more than half of the system went down","");
		}
		System.out.println("Sorting Task finished!!");
		result 								= doMerge(sortResult);
		if(result.status!=false) 
			printSummary(sortResult,mergeDuration,mergeCount);
		Util.getInstance().cleanIntermediateFiles(computeNodes.get(0).ip,computeNodes.get(0).port);
		return result;	
	}
	
	private JobStatus sortAndMerge(ArrayList< sortJob > jobs) throws TException
	{
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolCount);
		for(int i=0;i<jobs.size();i++) 
		{
			System.out.println("Starting Job with id " + jobs.get(i).id + " on node " + jobs.get(i).ip);
			executor.execute(jobs.get(i));
		}
		

		while(true)
		{
			int finishedJobs	= 0;
			for(int i=0;i<jobs.size();i++)
			{
				if(1==jobs.get(i).threadRunStatus) finishedJobs	= finishedJobs+1;
				if(2==jobs.get(i).threadRunStatus) 
				{
					sortFailedJobs		= sortFailedJobs + 1;
					sortJob retry		= reAssignSortJob(jobs.get(i));
					jobs.remove(i);
					jobs.add(retry);
					executor.execute(retry);
				}
			}
			if(finishedJobs==jobs.size()) break;
		}	
		executor.shutdown();

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
		
		int seed 							= (int)((long)System.currentTimeMillis() % 1000);
        Random rnd 							= new Random(seed);
		mergeJobs							= 0;
		int iter 							= 0;	
		while(true)
		{	
			ExecutorService executor 		= Executors.newFixedThreadPool(threadPoolCount/10);
			mjobs.clear();
			int finishedJobs				= 0;
			int task_node_idx				= rnd.nextInt(computeNodes.size());
			System.out.println("Started Fresh Round of Merging ....");
			for(int i=0;i<intermediateFiles.size();i=i+mergeTaskSize)
			{
				List<String> tFiles			= new ArrayList<String>();
				for(int j=i;j<i+mergeTaskSize && j<intermediateFiles.size();j++) 
					tFiles.add(intermediateFiles.get(j));
				mergeJob cmergeJob			= new mergeJob(iter,tFiles,computeNodes.get(task_node_idx).ip,computeNodes.get(task_node_idx).port);
				mjobs.add(cmergeJob);
				iter 						= iter+1;
			}	
			
			for(int i=0;i<mjobs.size();i++) executor.execute(mjobs.get(i));
			while(true)
			{
				finishedJobs		= 0;
				for(int i=0;i<mjobs.size();i++)
				{
					if(1==mjobs.get(i).threadRunStatus) finishedJobs = finishedJobs+1;
        	    	if(2==mjobs.get(i).threadRunStatus)
        	   	 	{
						mergeJobs			= mergeJobs+1;
						mergeFailedJobs  	= mergeFailedJobs + 1;
            	    	mergeJob retry   	= reAssignMergeJob(mjobs.get(i));
                		mjobs.remove(i);
                		mjobs.add(retry);
                		executor.execute(retry);
        			}
				}
				if(finishedJobs==mjobs.size()) break;
			}
			executor.shutdown();
			intermediateFiles.clear();
			mergeJobs						= mergeJobs + mjobs.size();
			for(int i=0;i<mjobs.size();i++) 
			{
					if(mergeCount.containsKey(mjobs.get(i).ip)==false) 
					{
							mergeCount.put(mjobs.get(i).ip,0);
							mergeDuration.put(mjobs.get(i).ip,(long)0L);
					}
					mergeCount.put(mjobs.get(i).ip,mergeCount.get(mjobs.get(i).ip)+1);
					mergeDuration.put(mjobs.get(i).ip,mergeDuration.get(mjobs.get(i).ip) + mjobs.get(i).result.time);
					intermediateFiles.add(mjobs.get(i).result.filename);
			}
			if(intermediateFiles.size()==1) break;
		}

		result.status	= true;
		result.filename	= intermediateFiles.get(0);
		System.out.println("Sorted output is stored in " + result.filename);
		String absolutePath	= System.getProperty("user.dir");
		new File(absolutePath+ "/" + intermediateDirectory + result.filename).renameTo(new File(absolutePath +"/" + outputDirectory + result.filename));
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

	sortJob reAssignSortJob(sortJob job)
	{
			int seed 			= (int)((long)System.currentTimeMillis() % 1000);
			Random rnd 			= new Random(seed);
			int retry_task_idx  = rnd.nextInt(computeNodes.size());
			System.out.println("Sort Task with Id " + job.id + " will be re-assigned to node with IP " + computeNodes.get(retry_task_idx).ip);	
			sortJob retry		= new sortJob(job.id,job.filename,job.offSet,job.numToSort,computeNodes.get(retry_task_idx).ip,computeNodes.get(retry_task_idx).port);
			return retry;
	}

	mergeJob reAssignMergeJob(mergeJob job)
	{
			int seed 			= (int)((long)System.currentTimeMillis() % 1000);
			Random rnd 			= new Random(seed);
			int retry_task_idx  = rnd.nextInt(computeNodes.size());
			System.out.println("Merge Task with Id " + job.id + " will be re-assigned to node with IP " + computeNodes.get(retry_task_idx).ip);	
			mergeJob retry		= new mergeJob(job.id,job.files,computeNodes.get(retry_task_idx).ip,computeNodes.get(retry_task_idx).port);
			return retry;
	}

	private void printSummary(ArrayList< sortJob > jobs,HashMap<String,Long> mergeDuration,HashMap<String,Integer> mergeCount)
	{
		HashMap<String,Long> taskSummary							= new HashMap<String,Long>();
		HashMap<String,Integer> taskCount							= new HashMap<String,Integer>();
		for(int i=0;i<jobs.size();i++)
		{
			if(taskSummary.containsKey(jobs.get(i).ip)==false)
			{
				taskSummary.put(jobs.get(i).ip,(long)0L);
				taskCount.put(jobs.get(i).ip,0);
			}
			taskSummary.put(jobs.get(i).ip,taskSummary.get(jobs.get(i).ip) + jobs.get(i).result.time);
			taskCount.put(jobs.get(i).ip,taskCount.get(jobs.get(i).ip)+1);
		}

		System.out.println("\n\nSummary of the Task:-");
		System.out.println("Number of Nodes used for processing:- 		" + initSystemSize);
		System.out.println("Number of Nodes failed during processing:- 	" + (initSystemSize-taskSummary.size()));
		System.out.println("Number of Sort Jobs Spawned:- 			" + (jobs.size()+sortFailedJobs));
		System.out.println("Number of Sort Jobs failed:-  			" + sortFailedJobs);
		System.out.println("Number of Merge Jobs Spawned:-			" + mergeJobs);
		System.out.println("Number of Merge Jobs failed:-			" + mergeFailedJobs);
		System.out.println("Summary of Sort Tasks by Compute Nodes:-	");
		System.out.println("\n---------------------------------------------");
		System.out.println("    HostName			Total Tasks	Total Time(in milli-seconds)  ");
		Iterator it1	= taskSummary.entrySet().iterator();
		Iterator it2    = taskCount.entrySet().iterator();
		while(it1.hasNext())
		{
			Map.Entry p1 	= (Map.Entry)it1.next();
			Map.Entry p2	= (Map.Entry)it2.next();
			System.out.println(" " + p1.getKey() + " 	   " + taskCount.get(p1.getKey()) + " 		   " + p1.getValue());
		}
		System.out.println("---------------------------------------------\n");
		System.out.println("Summary of Merge Tasks by Compute Nodes:-	");
		System.out.println("\n---------------------------------------------");
		System.out.println("    HostName			Total Tasks	Total Time(in milli-seconds)  ");
		Iterator it3	= mergeDuration.entrySet().iterator();
		Iterator it4    = mergeCount.entrySet().iterator();
		while(it3.hasNext())
		{
			Map.Entry p1 	= (Map.Entry)it3.next();
			Map.Entry p2	= (Map.Entry)it4.next();
			System.out.println(" " + p1.getKey() + " 	   " + mergeCount.get(p1.getKey()) + " 		   " + p1.getValue());
		}
		System.out.println("---------------------------------------------\n");
	}
}
