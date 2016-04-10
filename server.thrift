include "node.thrift"
service SortService
{
	string doJob(1:string filename),
	bool join(1:node.Node node)
}
