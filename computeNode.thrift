include "time.thrift"

service Compute
{
	time.Time doSort(1:string filename,2:i32 offset,3:i32 toSort),
	time.Time doMerge(1:list<string> files),
	bool ping()
}
