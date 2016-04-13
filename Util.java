import java.util.*;
import java.io.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class Util
{
	private static int MOD 		= 107;
	private static Util util 	= null;

	//Creating Singleton instance of the class
	public static Util getInstance()
	{
		if(util==null)
			util	= new Util();
		return util;
	}

        //Function to generate hash value for given string
    public static long hash(String input)
    {
    	long hash = 5381;
        for (int i = 0; i < input.length() ;i++)
       	{
        	hash = ((hash << 11) + hash) + input.charAt(i)*26*(i+1);
            hash = hash%MOD;
        }
        return hash;
    }
	
	public static TThreadPoolServer getServer(int Port,SortServiceHandler sortService) throws TTransportException
	{
		TServerTransport serverTransport    = new TServerSocket(Port);
        TTransportFactory factory           = new TFramedTransport.Factory();
        SortService.Processor processor   	= new SortService.Processor(sortService);
        TThreadPoolServer.Args args         = new TThreadPoolServer.Args(serverTransport);
        args.processor(processor);
        args.transportFactory(factory);
		return new TThreadPoolServer(args);
	}

	public static TThreadPoolServer getComputeServer(int Port,ComputeServiceHandler computeService) throws TTransportException
	{
		TServerTransport serverTransport    = new TServerSocket(Port);
        TTransportFactory factory           = new TFramedTransport.Factory();
        ComputeService.Processor processor  = new ComputeService.Processor(computeService);
        TThreadPoolServer.Args args         = new TThreadPoolServer.Args(serverTransport);
        args.processor(processor);
        args.transportFactory(factory);
		return new TThreadPoolServer(args);
	}
	
	public static HashMap<String,String> getParameters(String filename)
	{
		BufferedReader br	= null;
		String content		= "";
		HashMap<String,String> params	= new HashMap<String,String>();
		try
		{
			br				= new BufferedReader(new FileReader(filename));
			while((content = br.readLine())!=null)
			{
				String [] tokens 	= content.split(":");
				params.put(tokens[0],tokens[1]);
			}
		}
		catch(IOException e) {}
		finally
		{
			try
			{
				if(br!=null) br.close();
			}
			catch(IOException e){}
		}
		return params;
	}
	
	//Utility function to print nodes that are curently part of the system
	public static void printNodeList(List<Node> activeNodes)
	{
		System.out.println("Currently Nodes connected to Server ... ");
		System.out.println("---------------------------------------------------------");
		System.out.println("        HostName               Port     	             ");
		System.out.println("---------------------------------------------------------");
		for(int i=0;i<activeNodes.size();i++)
		{
			System.out.println(activeNodes.get(i).ip + "    " + activeNodes.get(i).port);
			System.out.println("---------------------------------------------------------");
		}
	}

        public static String hashFile(List<String> files){

                long code = 0;
                for(int i = 0 ; i < files.size(); i++){
                        String fileName = files.get(i);
                        code += Integer.parseInt(fileName.substring(fileName.lastIndexOf('_') + 1));
                }
                return String.valueOf(code);
        }

}
