include "node.thrift"
include "JobStatus.thrift"
service SortService
{
	JobStatus.JobStatus doJob(1:string filename),
	bool join(1:node.Node node)
}
