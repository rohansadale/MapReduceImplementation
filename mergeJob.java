import java.io.*;
import java.util.*;
import java.lang.System;
import java.lang.Runnable;
import java.util.concurrent.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;

public class mergeJob extends Thread
{
		public List<String> files;
		public String ip;
		public int port;
		public JobTime result;
		public int threadRunStatus;
		public int id;

		public mergeJob(int id,List<String> files,String ip,int port)
		{
				this.id					= id;
				this.ip					= ip;
				this.port				= port;
				this.result				= null;
				this.threadRunStatus	= 0;
				this.files				= files;
		}

		public void run()
		{
				try
				{
						TTransport transport				= new TSocket(ip,port);
						TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
						ComputeService.Client client		= new ComputeService.Client(protocol);
						transport.open();
						this.result							= client.doMerge(files);
						transport.close();
						if(this.result.time==-1)
								this.threadRunStatus = 2;
						else
								this.threadRunStatus = 1;
				}

				catch(TException x)
				{
						x.printStackTrace();
						this.threadRunStatus				= 2;
				}
		}
}