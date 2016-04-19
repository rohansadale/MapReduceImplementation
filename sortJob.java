import java.io.*;
import java.util.*;
import java.lang.System;
import java.lang.Runnable;
import java.util.concurrent.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;

public class sortJob implements Runnable
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

		@Override
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
						if(this.result.time==-1)
								this.threadRunStatus = 2;
						else
								this.threadRunStatus = 1;
				}

				catch(TException x)
				{
						this.threadRunStatus				= 2;
				}
		}
}
