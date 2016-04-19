include "JobTime.thrift"

service ComputeService
{
	JobTime.JobTime doSort(1:string filename,2:i32 offset,3:i32 toSort,4:string id),
	JobTime.JobTime doMerge(1:list<string> files,2:string id),
	bool cleanJob(),
	bool ping()
}
