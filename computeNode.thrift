include "JobTime.thrift"

service ComputeService
{
	JobTime.JobTime doSort(1:string filename,2:i32 offset,3:i32 toSort),
	JobTime.JobTime doMerge(1:list<string> files),
	string cleanSort(1:bool shouldDelete,2:string filename,3:i32 offset),
	string cleanMerge(1:bool shouldDelete,2:list<string> files),
	bool ping()
}
