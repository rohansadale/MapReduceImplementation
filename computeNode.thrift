include "JobTime.thrift"

service ComputeService
{
	JobTime.JobTime doSort(1:string filename,2:i32 offset,3:i32 toSort,4:string id),
	JobTime.JobTime doMerge(1:list<string> files,2:string id),
	JobTime.JobTime stopJob(1:string jobId,2:i32 taskId,3:i32 replId),
	bool cleanJob(),
	bool ping()
}
