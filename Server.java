import java.io.*;
import java.util.*;
import java.net.InetAddress;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import java.net.UnknownHostException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

public class Server
{
	private static String CONFIG_FILE_NAME				= "";
	private static String CURRENT_NODE_IP				= "";
	private static String SERVER_PORT_KEY				= "ServerPort";
	private static String SERVER_IP_KEY					= "ServerIP";
	private static String INPUT_DIRECTORY_KEY			= "InputDirectory";
	private static String INTERMEDIATE_DIRECTORY_KEY	= "IntermediateDirectory";
	private static String OUTPUT_DIRECTORY_KEY			= "OutputDirectory";
	private static String CHUNK_SIZE_KEY				= "ChunkSize";
	private static String MERGE_SIZE_KEY				= "MergeTaskSize";
	private static String TASK_REPLICATION_KEY			= "TaskReplication";
	
	public static void main(String targs[]) throws TException
	{
		try
		{
			CURRENT_NODE_IP			= InetAddress.getLocalHost().getHostName();
		}
		catch(Exception e)
		{
			System.out.println("Unable to get hostname ....");
		}

		if(CURRENT_NODE_IP=="")
		{
			System.out.println("Unable to get Current System's IP");
			return;
		}
		if(targs.length==1)
		{
			CONFIG_FILE_NAME					= targs[0];
		}
		else
		{
			System.out.println("Config file missing!!!");
			return;
		}

		HashMap<String,String> configParam	= Util.getInstance().getParameters(CONFIG_FILE_NAME);
		Node currentNode					= new Node(Integer.parseInt(configParam.get(SERVER_IP_KEY)),Integer.parseInt(configParam.get(SERVER_PORT_KEY)));
		SortServiceHandler sortService		= new SortServiceHandler(currentNode,configParam.get(INPUT_DIRECTORY_KEY),configParam.get(INTERMEDIATE_DIRECTORY_KEY),
																	configParam.get(OUTPUT_DIRECTORY_KEY),Integer.parseInt(configParam.get(CHUNK_SIZE_KEY)),
																	Integer.parseInt(configParam.get(MERGE_SIZE_KEY)),Integer.parseInt(configParam.get(TASK_REPLICATION_KEY));
		TThreadPoolServer server			= Util.getInstance().getQuorumServer(Integer.parseInt(configParam.get(SERVER_PORT_KEY)),sortService);
		server.serve();	
	}
}
