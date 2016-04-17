include "JobTime.thrift"

service ComputeService
{
	JobTime.JobTime doSort(1:string jobId,2:i32 taskId,3:i32 replId,4:string filename,5:i32 offset,6:i32 toSort),
	JobTime.JobTime doMerge(1:string jobId,2:i32 taskId,3:i32 replId,4:list<string> files),
	JobTime.JobTime stopJob(1:string jobId,2:i32 taskId,3:i32 replId),
	JobTime.JobTime completeJob(1:map<JobTime.JobTime,bool> action),
	void cleanJob(1:string JobId),
	bool ping()
}
