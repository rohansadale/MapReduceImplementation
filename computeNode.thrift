service Compute
{
	string doSort(1:string filename,2:i32 offset,3:i32 toSort),
	string doMerge(1:list<string> files)
}
