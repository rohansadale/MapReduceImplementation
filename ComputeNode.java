import java.io.*;
import java.util.*;
import java.net.InetAddress;
import org.apache.thrift.TException;
import org.apache.thrift.transport.*;
import java.net.UnknownHostException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class ComputeNode{

		private static String CONFIG_FILE_NAME = "";
		private static String CURRENT_NODE_IP = "";
		private static int CURRENT_NODE_PORT = 9091;
		private static String SERVER_PORT_KEY = "ServerPort";
		private static String SERVER_IP_KEY = "ServerIP";
		private static String INPUT_DIRECTORY_KEY = "InputDirectory";
		private static String INTERMEDIATE_DIRECTORY_KEY = "IntermediateDirectory";
		private static String OUTPUT_DIRECTORY_KEY = "OutputDirectory";
		private static String CHUNK_SIZE_KEY = "ChunkSize";
		private static String MERGE_SIZE_KEY = "MergeTaskSize";
		private static List<Node> activeNodes = null;


		public static void main(String targs[]) throws TException{
				try{
						CURRENT_NODE_IP = InetAddress.getLocalHost().getHostName();
				}catch(Exception e){
						System.out.println("Unable to get hostname ... ");
				}

				if(CURRENT_NODE_IP == ""){
						System.out.println("Unable to get System IP for this Node");
						return;
				}

				if(targs.length >= 2){
						CONFIG_FILE_NAME = targs[0];
						CURRENT_NODE_PORT = Integer.parseInt(targs[1]);
				}
				else{
						System.out.println("Config File Missing!");
						return;
				}

				HashMap<String, String> configParam = Util.getInstance().getParameters(CONFIG_FILE_NAME);
				boolean hasRegistered = false;

				try{
						// Establishing connection with Master and joining the system
						TTransport transport 				= new TSocket(configParam.get(SERVER_IP_KEY), Integer.parseInt(configParam.get(SERVER_PORT_KEY)));
						TProtocol protocol 					= new TBinaryProtocol(new TFramedTransport(transport));
						SortService.Client client 			= new SortService.Client(protocol);
						transport.open();
						hasRegistered 						= client.join(new Node(CURRENT_NODE_IP,CURRENT_NODE_PORT));
						transport.close();

				}catch(TException e){

						System.out.println(" =================== Unable to establish connection with Coordinator ... Exiting ... =================");
				}

				if(hasRegistered)
				{
						// Handler Code here
						ComputeServiceHandler computeService = new ComputeServiceHandler(configParam.get(INPUT_DIRECTORY_KEY),
										configParam.get(INTERMEDIATE_DIRECTORY_KEY),
										configParam.get(OUTPUT_DIRECTORY_KEY));

						TThreadPoolServer server 			= Util.getInstance().getComputeServer(CURRENT_NODE_PORT,computeService);
						System.out.println("Starting Compute Node at " + CURRENT_NODE_IP + " and Port " + CURRENT_NODE_PORT + "  ....");
						server.serve();
				}
				else
				{
						System.out.println("Unable to Register with Master");
						return;
				}				
		}
}
