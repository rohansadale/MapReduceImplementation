import java.io.*;
import java.util.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.TServer;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.TException;

public class Client
{
	private static String CONFIG_FILE_NAME				= "";
	private static String COORDINATOR_IP_KEY			= "ServerIP";
	private static String COORDINATOR_PORT_KEY			= "ServerPort";

	public static void main(String targs[]) throws TException
	{
		String inputFile								= "";
		if(targs.length>=1) CONFIG_FILE_NAME 			= targs[0];
		else
		{
			System.out.println("Config File Missing!!!");
			return;
		}
		if(targs.length>=2) inputFile					= targs[1];
		else
		{
			System.out.println("Input File Missing !!!");
			return;
		}
		JobStatus status						= new JobStatus(true,"","");
		HashMap<String,String> configParam  	= Util.getInstance().getParameters(CONFIG_FILE_NAME);
		try
		{
			TTransport transport				= new TSocket(configParam.get(COORDINATOR_IP_KEY),Integer.parseInt(configParam.get(COORDINATOR_PORT_KEY)));
			TProtocol protocol					= new TBinaryProtocol(new TFramedTransport(transport));
			SortService.Client client           = new SortService.Client(protocol);
            transport.open();
            status								= client.doJob(inputFile);
            transport.close();
		}
		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with Coordinator ... Exiting ... =================");
		}
		System.out.println("Output file is " + status.filename);
	}
}
