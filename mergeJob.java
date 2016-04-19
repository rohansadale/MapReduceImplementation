import java.io.*;
import java.util.*;
import java.lang.System;
import java.lang.Runnable;
import java.util.concurrent.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;

public class mergeJob implements Runnable
{
		public List<String> files;
		public String ip;
		public int port;
		public JobTime result;
		public int threadRunStatus;
		public int id;
		public String jobId;
		public int taskId;
		public int replId;

		public mergeJob(String jobId,int taskId,int replId,List<String> files,String ip,int port)
		{
			this.jobId 				= jobId;
			this.taskId				= taskId;
			this.replId				= replId;
			this.id					= id;
			this.ip					= ip;
			this.port				= port;
			this.result				= null;
			this.threadRunStatus	= 0;
			this.files				= files;
		}

		@Override
		public void run()
		{
				TTransport transport 						= null;
				try
				{
						transport							= new TSocket(ip,port);
						TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
						ComputeService.Client client		= new ComputeService.Client(protocol);
						transport.open();
						this.result							= client.doMerge(files,"1_"+jobId+"_"+taskId+"_"+replId);
						transport.close();
						if(this.result.time==-1)
								this.threadRunStatus = 2;
						else
								this.threadRunStatus = 1;
				}

				catch(TException x)
				{
						this.threadRunStatus				= 2;
						transport.close();
				}
		}
}
