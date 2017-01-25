{
	Page* curPage;
	bufMgr->readPage(file, pid, curPage);
	LeafNodeInt* curNode = reinterpret_cast<LeafNodeInt*> (curPage);
	int pos = INTARRAYLEAFSIZE;
	for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
	{
		assert(curNode->validArray[i]);
		if(curNode->keyArray[i] > key)
		{
			pos = i;
			break;
		}
	}

	int keyBuffer[INTARRAYLEAFSIZE + 1];
	RecordId ridBuffer[INTARRAYLEAFSIZE + 1];
	for(int i = 0; i < pos; ++i)
	{
		keyBuffer[i] = curNode->keyArray[i];
		ridBuffer[i] = curNode->ridArray[i]
	}
	keyBuffer[pos] = key;
	ridBuffer[pos] = rid;
	for(int i = pos + 1; i < INTARRAYLEAFSIZE + 1; ++i)
	{
		keyBuffer[i] = curNode->keyArray[i - 1];
		ridBuffer[i] = curNode->ridArray[i - 1];
		assert(curNode->validArray[i - 1]);
	}

	// allocate a new page
	PageId newPid;
	Page* newPage;
	bufMgr->allocatePage(file, newPid, newPage);
	LeafNodeInt* newNode = reinterpret_cast<LeafNodeInt*> (newPage);
	int k;
	for(k = 0; k < (INTARRAYLEAFSIZE + 1)/2; ++k)
	{
		curNode->keyArray[k] = keyBuffer[k];
		curNode->ridArray[k] = ridBuffer[k];
		curNode->validArray[k] = true;
	}
	for(int i = k; i < INTARRAYLEAFSIZE; ++i)
		curNode->validArray[k] = false;

	
	int m;
	for(m = 0; m < INTARRAYLEAFSIZE + 1 - (INTARRAYLEAFSIZE + 1)/2; ++m)
	{
		newNode->keyArray[m] = keyBuffer[m+k];
		newNode->ridArray[m] = ridBuffer[m+k];
		newNode->validArray[m] = true;
	}

	for(int i = m; i < INTARRAYLEAFSIZE; ++i)
		newNode->validArray[i] = false;
	// update the right sibling ptr
	newNode->rightSibPageNo = curNode->rightSibPageNo;
	curNode->rightSibPageNo = newPid;
	// update feedback tuple
	feedbackTuple.set(newPid, newNode->keyArray[0]);
	// unpin both pages
	bufMgr->unPinPage(file, pid, true);
	bufMgr->unPinPage(file, newPid, true);
}