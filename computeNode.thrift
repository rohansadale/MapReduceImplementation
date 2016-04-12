include "JobTime.thrift"

service ComputeService
{
	JobTime.JobTime doSort(1:string filename,2:i32 offset,3:i32 toSort),
	JobTime.JobTime doMerge(1:list<string> files),
	bool ping()
}
