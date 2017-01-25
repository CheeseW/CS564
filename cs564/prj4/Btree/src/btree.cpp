/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/bad_scan_param_exception.h"

#include <cassert>


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType):
		bufMgr(bufMgrIn),
		attributeType(attrType),
		attrByteOffset(attrByteOffset)
{
	// construct the index file name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	assert(attributeType == Datatype::INTEGER);
	// std::cout << "attrByteOffset = " << attrByteOffset << std::endl;

	// check if the index file exists
	if(!file->exists(outIndexName))
	{
		BlobFile* indexFile = new BlobFile(outIndexName, true);
		file = dynamic_cast<File*> (indexFile);
		assert(file != NULL);
		// write the IndexMetaInfo into the file
		// file->allocPage(headerPageNum);
		Page* pagePtr;
		bufMgr->allocPage(file, headerPageNum, pagePtr);
		assert(headerPageNum == 1);
		// std::cout << "header(indexMeta) pageId = " << headerPageNum << std::endl;
		rootPageNum = 0;

		// TODO: change outIndexName from string to char[20]
		IndexMetaInfo* indexMeta = reinterpret_cast<IndexMetaInfo*> (pagePtr);
		assert(outIndexName.length() + 1 <= 20);
		strcpy(indexMeta->relationName, outIndexName.c_str());
		indexMeta->attrByteOffset = attrByteOffset;
		indexMeta->attrType = attributeType;
		indexMeta->rootPageNo = rootPageNum;
		indexMeta->hasNonLeaf = false;
		// unpin the header page
		// std::cout << "before unPinPage headerPage in BTreeIndex" << std::endl;
		bufMgr->unPinPage(file, headerPageNum, true);

		// std::cout << "rootPageNum = " << rootPageNum << std::endl;
		// Page* indexMetaPage = reinterpret_cast<Page*> (&indexMeta);
		// file->writePage( headerPageNum, indexMetaPage);

		// at start has no non-leaf nodes
		hasNonLeaf = false;
		
		// scan the relation & insert tuples
		FileScan filescanner = FileScan(relationName, bufMgr);
		bool scanComplete = false;
		while(!scanComplete)
		{
			try {
				RecordId curRecId;
				filescanner.scanNext(curRecId);
				std::string recordData = filescanner.getRecord();
				// std::string keyData = recordData.substr(attrByteOffset, sizeof(int));
				const char* data_str = recordData.c_str();
				// key type is integer by default
				// int* key = reinterpret_cast<int*> (&keyData);
				insertEntry(static_cast<const void*> (data_str + attrByteOffset), curRecId);
			}
			catch (EndOfFileException) {
				scanComplete = true;
			}
		}
	}
	else
	{
		BlobFile* indexFile = new BlobFile(outIndexName, false);
		assert(indexFile != NULL);
		file = dynamic_cast<File*> (indexFile);
		// retrieve the IndexMetaInfo from the index header page
		headerPageNum = 1; /* by default */
		// Page indexMetaPage = file->readPage(headerPageNum);
		Page* indexMetaPage;
		bufMgr->readPage(file, headerPageNum, indexMetaPage);
		IndexMetaInfo* indexMeta = reinterpret_cast<IndexMetaInfo*> (indexMetaPage);
		assert(indexMeta->attrByteOffset == attrByteOffset);
		assert(indexMeta->attrType == attributeType);
		rootPageNum = indexMeta->rootPageNo;
		hasNonLeaf = indexMeta->hasNonLeaf;
		// unpin the header page
		// std::cout << "before unPinPage headerPage 2 in BTreeIndex" << std::endl;
		bufMgr->unPinPage(file, headerPageNum, false);
	}

	scanExecuting = false;
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// do not delete the index file in this function
	// TODO construct a test case that destructs a B+ tree index and then reuse the file
	// all pages should be unpinned right after use. only need to flush here
	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// interpret the key as integer by default
	// be sure to update the IndexMetaInfo
	int intKey = *(static_cast< const int*> (key));
	// std::cout << "insert key = " << intKey << std::endl;
	bool needsUpate = false;
	PageKeyPair<int> feedbackTuple;
	insertEntryUpdate(intKey, rid, rootPageNum, true, needsUpate, feedbackTuple);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	lowValInt = *(static_cast<const int*>(lowValParm));
	highValInt = *(static_cast<const int*>(highValParm));
	// lowValInt = *(static_cast<int*>(lowValParm));
	// highValInt = *(static_cast<int*>(highValInt));
	if(lowValInt > highValInt)
		throw BadScanParamException();
	lowOp = lowOpParm;
	highOp = highOpParm;
	if (!(lowOp == Operator::GT || lowOp == Operator::GTE) )
		throw BadOpcodesException();
	if(!(highOp == Operator::LT || highOp == Operator::LTE) )
		throw BadOpcodesException();

	scanExecuting = true;

	if(hasNonLeaf)
	{
		Page* page;
		NonLeafNodeInt* node;
		PageId pageId = rootPageNum;
		bufMgr->readPage(file, pageId, page);
		node = reinterpret_cast<NonLeafNodeInt*> (page);
		while(node->level == 0)
		{	
			int i;
			for(i = 0; i < INTARRAYNONLEAFSIZE; ++i)
			{
				if(node->validArray[i] && node->keyArray[i] > lowValInt)
					break;
			}
			if(i == INTARRAYNONLEAFSIZE)
			{
				for(i = 0; i < INTARRAYNONLEAFSIZE; ++i)
				{
					if(node->validArray[i] == false)
						break;
				}
				assert(i - 1 >= 0);
				assert(node->validArray[i - 1]);
			}
			assert(i < INTARRAYNONLEAFSIZE + 1);
			
			// std::cout << "node-level = 0" << std::endl;
			// if(i == INTARRAYNONLEAFSIZE)
			// 	std::cout << "selected key > " << node->keyArray[i - 1] << std::endl;
			// else
			// 	std::cout << "selected key >=" << node->keyArray[i - 1] << ", < " << node->keyArray[i] << std::endl;
			
			// std::cout << "show non-leaf node" << std::endl;
			// for(int x = 0; x < INTARRAYNONLEAFSIZE; ++x)
			// {
			// 	if(node->validArray[x])
			// 		std::cout << x << " ";
			// }
			// std::cout << std::endl;
			// for(int x = 0; x < INTARRAYNONLEAFSIZE; ++x)
			// {
			// 	if(node->validArray[x])
			// 		std::cout << node->keyArray[x] << " ";
			// }
			// std::cout << std::endl << std::endl;

			PageId oriPid = pageId;
			pageId = node->pageNoArray[i];
			// unpin the non-leaf page
			bufMgr->unPinPage(file, oriPid, false);
			bufMgr->readPage(file, pageId, page);
			node = reinterpret_cast<NonLeafNodeInt*> (page);
		}

		// search the level-1 non-leaf node for the leaf node
		int pos;
		for(pos = 0; pos < INTARRAYNONLEAFSIZE; ++pos)
		{
			if(node->validArray[pos] && node->keyArray[pos] > lowValInt)
				break;
		}
		if(pos == INTARRAYNONLEAFSIZE)
		{
			for(pos = 0; pos < INTARRAYNONLEAFSIZE; ++pos)
			{
				if(node->validArray[pos] == false)
					break;
			}
			--pos;
			assert(pos >= 0);
		}
		assert(pos < INTARRAYNONLEAFSIZE + 1);
		
		// std::cout << "node-level = 1" << std::endl;
		// std:: cout << "pos = " << pos << std::endl;
		// if(pos == INTARRAYNONLEAFSIZE)
		// 	std::cout << "selected key >= " << node->keyArray[pos - 1] << std::endl;
		// else
		// 	std::cout << "selected key >=" << node->keyArray[pos - 1] << ", < " << node->keyArray[pos] << std::endl;
		
		// std::cout << "show non-leaf node " << std::endl;
		// for(int x = 0; x < INTARRAYNONLEAFSIZE; ++x)
		// {
		// 	if(node->validArray[x])
		// 		std::cout << x << " ";
		// }
		// std::cout << std::endl;
		// for(int x = 0; x < INTARRAYNONLEAFSIZE; ++x)
		// {
		// 	if(node->validArray[x])
		// 		std::cout << node->keyArray[x] << " ";
		// }
		// std::cout << std::endl << std::endl;

		PageId oriPid = pageId;
		pageId = node->pageNoArray[pos];
		bufMgr->unPinPage(file, oriPid, false);
		currentPageNum = pageId;
		bufMgr->readPage(file, currentPageNum, currentPageData);
		nextEntry = 0;
	}
	else
	{
		// no non-leaf node
		// only set b+ tree fileds when there are records ever inserted
		if(rootPageNum != 0)
		{
			currentPageNum = rootPageNum;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			nextEntry = 0;
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

	assert(scanExecuting);

	// if no records has ever been inserted
	if(rootPageNum == 0)
	{
		assert(!hasNonLeaf);
		throw IndexScanCompletedException();
	}

	while(1)
	{
		LeafNodeInt* node = reinterpret_cast<LeafNodeInt*> (currentPageData);
		// for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
		// {
		// 	if(node->validArray[i])
		// 	{
		// 		std::cout << "key = " << node->keyArray[i] << ", ";
		// 	}
		// }
		// std::cout << std::endl;
		for(int i = nextEntry; i < INTARRAYLEAFSIZE; ++i)
		{
			if(node->validArray[i])
			{
				bool sat = true;
				bool end = false;

				if(highOp == Operator::LT)
					end = (node->keyArray[i] >= highValInt);
				else
					end = (node->keyArray[i] > highValInt);

				if(end)
					throw IndexScanCompletedException();

				if(lowOp == Operator::GT)
					sat = sat && (node->keyArray[i] > lowValInt);
				else
					sat = sat && (node->keyArray[i] >= lowValInt);

				if(highOp == Operator::LT)
					sat = sat && (node->keyArray[i] < highValInt);
				else
					sat = sat && (node->keyArray[i] <= highValInt);

				if(sat)
				{
					// std::cout << "thisEntry = " << i << std::endl;
					// std::cout << "sat key = " << node->keyArray[i] << std::endl;
					outRid = node->ridArray[i];
					nextEntry = i + 1;
					// std::cout << "nextEntry = " << nextEntry << std::endl;
					// std::cout << "rightSibPageNo = " << node->rightSibPageNo << std::endl << std::endl;
					return;
				}
			}
		}

		// std::cout << "out of page loop" << std::endl;

		if(node->rightSibPageNo == 0)
			throw IndexScanCompletedException();

		nextEntry = 0;
		// unpin the current page
		PageId oriPid = currentPageNum;
		currentPageNum = node->rightSibPageNo;
		bufMgr->unPinPage(file, oriPid, false);
		// read the next page in
		bufMgr->readPage(file, currentPageNum, currentPageData);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if(!scanExecuting)
		throw ScanNotInitializedException();

	// need to unpin the last page here
	bufMgr->unPinPage(file, currentPageNum, false);
	scanExecuting = false;
	nextEntry = 0;
	currentPageData = NULL;
}

// -----------------------------------------------------------------------------
// B+ Tree node update functions
// -----------------------------------------------------------------------------
void BTreeIndex::leafNodeInsert(LeafNodeInt* node, int key, RecordId rid)
{
	// // Page curPage = file->readPage(pid);
	// page* curPage;
	// // TODO: unpin this page later
	// bufMgr->readPage(file, pid, curPage);
	// LeafNodeInt* node = reinterpret_cast<LeafNodeInt*> (curPage);
	int i;
	for(i = 0; i < INTARRAYLEAFSIZE; ++i)
	{
		if(node->validArray[i] == false)
			break;

		if(node->validArray[i] == true && node->keyArray[i] > key)
			break;
	}
	// the node can't be full
	assert(i < INTARRAYLEAFSIZE);

	if(node->validArray[i] == false)
	{
		// the fit key position is at the end of the array
		node->validArray[i] = true;
		node->keyArray[i] = key;
		node->ridArray[i] = rid;
	}
	else
	{
		// assert(i > 0);
		for(int k = INTARRAYLEAFSIZE - 1; k > i; --k)
		{
			if(node->validArray[k - 1])
			{
				node->validArray[k] = node->validArray[k - 1];
				node->keyArray[k] = node->keyArray[k - 1];
				node->ridArray[k] = node->ridArray[k - 1];
			}
		}
		node->validArray[i] = true;
		node->keyArray[i] = key;
		node->ridArray[i] = rid;
	}

	// std::cout << "key = " << key << " inserted" << std::endl;
	// std::cout << "show leaf node after key insertion" << std::endl;
	// for(i = 0; i < INTARRAYLEAFSIZE; ++i)
	// {
	// 	if(node->validArray[i] == true)
	// 		std::cout << node->keyArray[i] << " ";
	// }
	// std::cout << std::endl << std::endl;
}

void BTreeIndex::leafNodeInsertSplit(LeafNodeInt* curNode, int key, RecordId rid, PageKeyPair<int>& feedbackTuple)
{	
	// std::cout << "key = " << key << " inserted" << std::endl;
	// std::cout << "show data in the leaf node before split" << std::endl;
	// for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
	// {
	// 	assert(curNode->validArray[i]);
	// 	std::cout << "key = " << curNode->keyArray[i] << " ";
	// }
	// std::cout << std::endl << std::endl;

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
		ridBuffer[i] = curNode->ridArray[i];
	}
	keyBuffer[pos] = key;
	ridBuffer[pos] = rid;
	for(int i = pos + 1; i < INTARRAYLEAFSIZE + 1; ++i)
	{
		keyBuffer[i] = curNode->keyArray[i - 1];
		ridBuffer[i] = curNode->ridArray[i - 1];
		assert(curNode->validArray[i - 1]);
	}

	// std::cout << std::endl << "show buffer" << std::endl;
	// for(int i = 0; i < INTARRAYLEAFSIZE + 1; ++i)
	// {
	// 	std::cout << keyBuffer[i] << " ";
	// }
	// std::cout << std::endl;

	// allocate a new page
	PageId newPid;
	Page* newPage;
	bufMgr->allocPage(file, newPid, newPage);
	LeafNodeInt* newNode = reinterpret_cast<LeafNodeInt*> (newPage);
	int k;
	for(k = 0; k < (INTARRAYLEAFSIZE + 1)/2; ++k)
	{
		curNode->keyArray[k] = keyBuffer[k];
		curNode->ridArray[k] = ridBuffer[k];
		curNode->validArray[k] = true;
	}
	for(int i = k; i < INTARRAYLEAFSIZE; ++i)
		curNode->validArray[i] = false;

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

	// std::cout << "show data in the leaf node after split in page 1" << std::endl;
	// for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
	// {
	// 	if(curNode->validArray[i])
	// 		std::cout << "key = " << curNode->keyArray[i] << " ";
	// }
	// std::cout << std::endl << std::endl;

	// std::cout << "show data in the leaf node after split in page 2" << std::endl;
	// for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
	// {
	// 	if(newNode->validArray[i])
	// 		std::cout << "key = " << newNode->keyArray[i] << " ";
	// }
	// std::cout << std::endl;
	// std::cout << "in leaf node feedbackTuple.newPid = " << newPid << std::endl;
	// std::cout << std::endl << std::endl;

	bufMgr->unPinPage(file, newPid, true);
	// std::cout << "key = " << key << " inserted" << std::endl;
}

void BTreeIndex::nonLeafNodeInsert(NonLeafNodeInt* node, const PageKeyPair<int>& updateTuple)
{
	// std::cout << "insert pageNo = " << updateTuple.pageNo << ", key = " << updateTuple.key << std::endl;
	// std::cout << "non-leaf node before insert" << ", level = " << node->level <<std::endl;
	// int x;
	// for(x = 0; x < INTARRAYNONLEAFSIZE; ++x)
	// {
	// 	if(node->validArray[x])
	// 		std::cout << "x = " << x << ", Pid = " << node->pageNoArray[x] << ", key = " << node->keyArray[x] << ", ";
	// 	else
	// 		break;
	// }
	// std::cout << "Pid = " << node->pageNoArray[x]; 
	// std::cout << std::endl << std::endl;

	int i;
	for(i = 0; i < INTARRAYNONLEAFSIZE; ++i)
	{
		if(node->validArray[i] == false)
			break;

		if(node->validArray[i] == true && node->keyArray[i] > updateTuple.key)
			break;
	}
	// the node can't be full
	assert(i < INTARRAYNONLEAFSIZE);

	if(node->validArray[i] == false)
	{
		// the fit key position is at the end of the array
		node->validArray[i] = true;
		node->keyArray[i] = updateTuple.key;
		// update the ptr at the right side of the key
		node->pageNoArray[i + 1] = updateTuple.pageNo;
	}
	else
	{
		// assert(i > 0);
		for(int k = INTARRAYNONLEAFSIZE - 1; k > i; --k)
		{
			if(node->validArray[k - 1])
			{
				node->validArray[k] = node->validArray[k - 1];
				node->keyArray[k] = node->keyArray[k - 1];
				node->pageNoArray[k+1] = node->pageNoArray[k];
			}
		}
		node->validArray[i] = true;
		node->keyArray[i] = updateTuple.key;
		node->pageNoArray[i+1] = updateTuple.pageNo;
	}

	// // unpin the page
	// bufMgr->unPinPage(file, pid, true);

	// std::cout << "non-leaf node after insert" << std::endl;
	// for(x = 0; x < INTARRAYNONLEAFSIZE; ++x)
	// {
	// 	if(node->validArray[x])
	// 		std::cout << "x = " << x << ", Pid = " << node->pageNoArray[x] << ", key = " << node->keyArray[x] << ", ";
	// 	else
	// 		break;
	// }
	// std::cout << "Pid = " << node->pageNoArray[x]; 
	// std::cout << std::endl << std::endl;
}

void BTreeIndex::nonLeafNodeInsertSplit(NonLeafNodeInt* curNode, const PageKeyPair<int>& updateTuple, PageKeyPair<int>& feedbackTuple)
{
	// std::cout << "insert split pageNo = " << updateTuple.pageNo << ", key = " << updateTuple.key << std::endl;
	// std::cout << "non-leaf node before insert split" << ", level = " << curNode->level << std::endl;
	// int x;
	// for(x = 0; x < INTARRAYNONLEAFSIZE; ++x)
	// {
	// 	if(curNode->validArray[x])
	// 		std::cout << "x = " << x << ", Pid = " << curNode->pageNoArray[x] << ", key = " << curNode->keyArray[x] << ", ";
	// 	else
	// 		break;
	// }
	// std::cout << "Pid = " << curNode->pageNoArray[x]; 
	// std::cout << std::endl << std::endl;


	int pos = INTARRAYNONLEAFSIZE;
	for(int i = 0; i < INTARRAYNONLEAFSIZE; ++i)
	{
		assert(curNode->validArray[i]);
		if(curNode->keyArray[i] > updateTuple.key)
		{
			pos = i;
			break;
		}
	}

	int keyBuffer[INTARRAYNONLEAFSIZE + 1];
	PageId pidBuffer[INTARRAYNONLEAFSIZE + 2];

	for(int i = 0; i < pos; ++i)
	{
		keyBuffer[i] = curNode->keyArray[i];
		// update the left side ptrs
		pidBuffer[i] = curNode->pageNoArray[i];
	}
	// keep the page ptr on the left side of the newly inserted node
	pidBuffer[pos] = curNode->pageNoArray[pos];
	keyBuffer[pos] = updateTuple.key;
	pidBuffer[pos + 1] = updateTuple.pageNo;
	for(int i = pos + 1; i < INTARRAYNONLEAFSIZE + 1; ++i)
	{
		keyBuffer[i] = curNode->keyArray[i - 1];
		// update the right side ptrs
		pidBuffer[i + 1] = curNode->pageNoArray[i];
		assert(curNode->validArray[i - 1]);
	}

	// allocate a new page
	PageId newPid;
	Page* newPage;
	bufMgr->allocPage(file, newPid, newPage);

	NonLeafNodeInt* newNode = reinterpret_cast<NonLeafNodeInt*> (newPage);
	int updateKey = keyBuffer[(INTARRAYNONLEAFSIZE + 1)/2];	// key used for upper level updates
	int k;
	for(k = 0; k < (INTARRAYNONLEAFSIZE + 1)/2; ++k)
	{
		curNode->keyArray[k] = keyBuffer[k];
		curNode->pageNoArray[k] = pidBuffer[k];
		curNode->validArray[k] = true;
	}
	// left partition keeps the ptr at the left side of the updateKey
	curNode->pageNoArray[k] = pidBuffer[k];
	for(int i = k; i < INTARRAYNONLEAFSIZE; ++i)
		curNode->validArray[i] = false;

	
	int m;
	// right partition keeps the ptr at the right side of the updateKey
	newNode->pageNoArray[0] = pidBuffer[k + 1];
	for(m = 0; m < INTARRAYNONLEAFSIZE - (INTARRAYNONLEAFSIZE + 1)/2; ++m)
	{
		newNode->keyArray[m] = keyBuffer[m + k + 1];
		newNode->pageNoArray[m + 1] = pidBuffer[m + k + 2];
		newNode->validArray[m] = true;
	}
	// int updateKeyMax = newNode->keyArray[0];	

	for(int i = m; i < INTARRAYNONLEAFSIZE; ++i)
		newNode->validArray[i] = false;

	// update the level of new non-leaf node
	newNode->level = curNode->level;


	// std::cout << "non-leaf node (ori) after insert split" << std::endl;
	// for(x = 0; x < INTARRAYNONLEAFSIZE; ++x)
	// {
	// 	if(curNode->validArray[x])
	// 		std::cout << "x = " << x << ", Pid = " << curNode->pageNoArray[x] << ", key = " << curNode->keyArray[x] << ", ";
	// 	else
	// 		break;
	// }
	// std::cout << "Pid = " << curNode->pageNoArray[x]; 
	// std::cout << std::endl << std::endl;
	// std::cout << "non-leaf node (new) after insert split" << std::endl;
	// for(x = 0; x < INTARRAYNONLEAFSIZE; ++x)
	// {
	// 	if(newNode->validArray[x])
	// 		std::cout << "x = " << x << ", Pid = " << newNode->pageNoArray[x] << ", key = " << newNode->keyArray[x] << ", ";
	// 	else
	// 		break;
	// }
	// std::cout << "Pid = " << newNode->pageNoArray[x]; 
	// std::cout << std::endl << "in non-leaf node feedbackTuple.newPid = " << newPid;
	// std::cout << std::endl << std::endl;

	// update feedback tuple
	feedbackTuple.set(newPid, updateKey);
	// only unpin the newly allocated page
	// std::cout << "before unPinPage in nonLeafNodeInsertSplit" << std::endl;
	bufMgr->unPinPage(file, newPid, true);
}

void BTreeIndex::insertEntryUpdate(int key, RecordId rid, PageId pid, bool isRoot, bool& needsUpate, PageKeyPair<int>& feedbackTuple)
{
	// the first call
	// special condition that there is no non-leaf node in the tree
	if(!hasNonLeaf)
	{
		assert(isRoot);
		// TODO: rememer to update the index meta info!!!!!
		// the first record inserted ever, need to allocate page
		if(rootPageNum == 0)
		{
			Page* rootPage;
			bufMgr->allocPage(file, rootPageNum, rootPage);
			assert(rootPageNum > 0);
			LeafNodeInt* node = reinterpret_cast<LeafNodeInt*> (rootPage);
			node->rightSibPageNo = 0; // 0 represents end of leaf node chain
			for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
				node->validArray[i] = false;
			leafNodeInsert(node, key, rid);
				
			// update the header page
			Page* headerPage;
			bufMgr->readPage(file, headerPageNum, headerPage);
			IndexMetaInfo* indexMeta = reinterpret_cast<IndexMetaInfo*> (headerPage);
			indexMeta->rootPageNo = rootPageNum;

			// unpin the root & header pages
			// std::cout << "before unPinPage headerPage in insertEntryUpdate" << std::endl;
			bufMgr->unPinPage(file, headerPageNum, true);
			// std::cout << "before unPinPage rootPageNum in insertEntryUpdate" << std::endl;
			bufMgr->unPinPage(file, rootPageNum, true);

			needsUpate = false;
			return;
		}
		else
		{
			// TODO: remember to update headerPage & unpin pages!!!
			// the leaf page already exists and is not full
			Page* rootPage;
			bufMgr->readPage(file, rootPageNum, rootPage);
			LeafNodeInt* node = reinterpret_cast<LeafNodeInt*> (rootPage);
			int vldCnt = 0;
			for(int i = 0; i < INTARRAYLEAFSIZE; ++i)
			{
				if(node->validArray[i])
					++vldCnt;
			}

			if(vldCnt == INTARRAYLEAFSIZE)
			{
				// the leaf node is full, need to allocate the first non-leaf node
				PageKeyPair<int> tuple;
				leafNodeInsertSplit(node, key, rid, tuple);
				// create a new root node, the key is in ftuple, and the 2 ptrs are in original root page number & tuple (right side)
				Page* newRootPage;
				PageId newRootPid;
				bufMgr->allocPage(file, newRootPid, newRootPage);
				NonLeafNodeInt* rootNode = reinterpret_cast<NonLeafNodeInt*> (newRootPage);
				rootNode->level = 1;	// just above the leaf nodes
				for(int i = 1; i < INTARRAYNONLEAFSIZE; ++i)
					rootNode->validArray[i] = false;
				rootNode->validArray[0] = true;
				rootNode->keyArray[0] = tuple.key;
				rootNode->pageNoArray[0] = rootPageNum;
				rootNode->pageNoArray[1] = tuple.pageNo;

				// update the header page
				Page* headerPage;
				bufMgr->readPage(file, headerPageNum, headerPage);
				IndexMetaInfo* indexMeta = reinterpret_cast<IndexMetaInfo*> (headerPage);
				indexMeta->rootPageNo = newRootPid;
				indexMeta->hasNonLeaf = true;

				// std::cout << "before unPinPage rootPage 2 in insertEntryUpdate" << std::endl;
				// unpin the original root page
				bufMgr->unPinPage(file, rootPageNum, true);

				// update the b+ tree fields
				rootPageNum = newRootPid;
				hasNonLeaf = true;

				// unpin pages (the original root page, new root page & header page)
				// std::cout << "before unPinPage headerPage 2 in insertEntryUpdate" << std::endl;
				bufMgr->unPinPage(file, headerPageNum, true);
				// std::cout << "before unPinPage new rootPage 2 in insertEntryUpdate" << std::endl;
				bufMgr->unPinPage(file, newRootPid, true);
				return;
			}
			else
			{
				// the leaf node is not full, only need to update the leaf node
				assert(vldCnt < INTARRAYLEAFSIZE);
				leafNodeInsert(node, key, rid);
				// unpin the root page
				// std::cout << "before unPinPage rootPage 3 in insertEntryUpdate" << std::endl;
				bufMgr->unPinPage(file, rootPageNum, true);
				return;
			}
		}
	}

	// conditions below must have non-leaf nodes
	// pid is the PageId of the current page
	// Remember to upin pages!!!
	Page* page;
	bufMgr->readPage(file, pid, page);	
	NonLeafNodeInt* node = reinterpret_cast<NonLeafNodeInt*> (page);

	// check if node is full
	int sum = 0;
	for(int k = 0; k < INTARRAYNONLEAFSIZE; ++k)
	{
		if(node->validArray[k])
			++sum;
	}

	int pos;
	for(pos = 0; pos < INTARRAYNONLEAFSIZE; ++pos)
	{
		if(node->validArray[pos] && node->keyArray[pos] > key)
			break;

		if(node->validArray[pos] == false)
		{
			// the node can not be empty
			if(pos == 0)
			{
				std::cout << "headerPageNum = " << headerPageNum << std::endl;
				std::cout << "rootPageNum = " << rootPageNum << std::endl;
				std::cout << "pid = " << pid << std::endl;
			}
			assert(pos > 0);
			break;
		}
	}

	if(pos >= INTARRAYNONLEAFSIZE || node->validArray[pos] == false)
	{
		// when pos > INTARRAYNONLEAFSIZE, the current node is full & the key is larger than any key in the node
		// take the right hand side PageId
		
		// pos > 0
		assert(node->validArray[pos - 1] == true);
		if(node->keyArray[pos - 1] > key)
		{
			std::cout << "valid = " << node->validArray[pos] << std::endl;
			std::cout << "pos = " << pos << std::endl;
			std::cout << "key = " << key << std::endl;
			std::cout << "keyArray[pos] = " << node->keyArray[pos] << std::endl;
		}
		assert(node->keyArray[pos - 1] <= key);
	}
	// if at level 1 (just above leaf), need to insert directly into the leaf nodes. recurssion reaches deepest level
	if(node->level == 1)
	{
		// if(pid == rootPageNum)
		// 	std::cout << "at root node, start of recurssion" << " ";
		// std::cout << "at level = 1" << std::endl;

		// assert(i < INTARRAYNONLEAFSIZE);
		PageId leafPid = node->pageNoArray[pos];

		// read the leaf page
		Page* leafPage;
		bufMgr->readPage(file, leafPid, leafPage);

		LeafNodeInt* leafNode = reinterpret_cast<LeafNodeInt*> (leafPage);
		int vldCnt = 0;
		for(int k = 0; k < INTARRAYLEAFSIZE; ++k)
		{
			if(leafNode->validArray[k])
				++vldCnt;
		}
		
		if(vldCnt < INTARRAYLEAFSIZE)
		{
			// if not full, just update the leaf page, do not need to change this level (level 1 non-leaf)
			leafNodeInsert(leafNode, key, rid);
			needsUpate = false;
		}
		else
		{
			// if full, split the leaf page and get back the feedback, and construct this level's feedback to upper level
			PageKeyPair<int> updateTuple;
			leafNodeInsertSplit(leafNode, key, rid, updateTuple);

			// check if this non-leaf node is full
			if(sum == INTARRAYNONLEAFSIZE)
			{
				// the non-leaf node is full, needs to update upper level
				needsUpate = true;
				nonLeafNodeInsertSplit(node, updateTuple, feedbackTuple);
				// std::cout << "in insertEntryUpdate level = 1 feedbackTuple.pageNo = " << feedbackTuple.pageNo << std::endl;
				// test only yanqi
				assert(feedbackTuple.pageNo != 0);
			}
			else
			{
				needsUpate = false;
				nonLeafNodeInsert(node, updateTuple);
			}
		}

		bufMgr->unPinPage(file, leafPid, true);
	}
	else
	{
		// if(pid == rootPageNum)
		// 	std::cout << "at root node, start of recurssion" << " ";
		// std::cout << "at level = 0" << std::endl;
		// at level 0 (intermediate), find the next level non-leaf node and do recurssion
		PageKeyPair<int> updateTuple;
		updateTuple.set(0, 0);
		PageId nextNonLeafPid = node->pageNoArray[pos];
		// if(nextNonLeafPid == 0)
		// {
		// 	assert(node->validArray[pos]);
		// }
		// assert(nextNonLeafPid != 0);
		bool needsUpdateThisLvl = false;
		insertEntryUpdate(key, rid, nextNonLeafPid, false, needsUpdateThisLvl, updateTuple);
		// if(pid == rootPageNum)
		// 	std::cout << "back at root, also ";
		// std::cout << "back at level = 0" << std::endl;

		if(needsUpdateThisLvl)
			assert(updateTuple.pageNo != 0);

		if(needsUpdateThisLvl)
		{
			// check if this non-leaf node is full
			if(sum == INTARRAYNONLEAFSIZE)
			{
				// the non-leaf node is full, needs to update upper level
				needsUpate = true;
				nonLeafNodeInsertSplit(node, updateTuple, feedbackTuple);

				// std::cout << "in insertEntryUpdate level = 0 feedbackTuple.pageNo = " << feedbackTuple.pageNo << std::endl;

				// test only yanqi
				assert(feedbackTuple.pageNo != 0);
			}
			else
			{
				needsUpate = false;
				nonLeafNodeInsert(node, updateTuple);
			}
		}
	}

	// std::cout << "before unPinPage 3 in insertEntryUpdate" << std::endl;
	bufMgr->unPinPage(file, pid, true);

	// std::cout << "at non-root node, needsUpate = " << needsUpate <<", ";
	// std::cout << "feedbackTuple.pageNo = " << feedbackTuple.pageNo << ", feedbackTuple.key = " << feedbackTuple.key << std::endl;
		
	// if at root level, need to use the feedback information to build a new root if necessary
	if(isRoot)
	{
		// std::cout << "at root, end of recurssion" << std::endl << std::endl;
		assert(pid == rootPageNum);
		// need to create a new root & update the header file
		if(needsUpate)
		{
			// std::cout << "one more level of node created" << std::endl;
			PageId newRootPid;
			Page* newRootPage;
			bufMgr->allocPage(file, newRootPid, newRootPage);

			NonLeafNodeInt* rootNode = reinterpret_cast<NonLeafNodeInt*> (newRootPage);
			rootNode->level = 0;
			for(int i = 1; i < INTARRAYNONLEAFSIZE; ++i)
				rootNode->validArray[i] = false;
			rootNode->validArray[0] = true;
			rootNode->keyArray[0] = feedbackTuple.key;
			rootNode->pageNoArray[0] = rootPageNum;
			rootNode->pageNoArray[1] = feedbackTuple.pageNo;

			// update the header page
			Page* headerPage;
			bufMgr->readPage(file, headerPageNum, headerPage);
			IndexMetaInfo* indexMeta = reinterpret_cast<IndexMetaInfo*> (headerPage);
			indexMeta->rootPageNo = newRootPid;
			assert(indexMeta->hasNonLeaf);

			// update the b+ tree fields
			rootPageNum = newRootPid;
			assert(hasNonLeaf = true);

			// std::cout << "show new root node" << std::endl;
			// int ii;
			// for( ii = 0; ii < INTARRAYNONLEAFSIZE; ++ii)
			// {
			// 	if(rootNode->validArray[ii])
			// 	{
			// 		std::cout << "ptr = " << rootNode->pageNoArray[ii] << "; key = " << rootNode->keyArray[ii] << "; ";
			// 	}
			// 	else
			// 		break;
			// }
			// if(rootNode->validArray[ii - 1])
			// 	std::cout << "ptr = " << rootNode->pageNoArray[ii] << std::endl;
			// std::cout << std::endl;

			// unpin pages (the original root page, new root page & header page)
			// std::cout << "before unPinPage 6 in insertEntryUpdate" << std::endl;
			bufMgr->unPinPage(file, headerPageNum, true);
			// std::cout << "before unPinPage headerPage 7 in insertEntryUpdate" << std::endl;
			bufMgr->unPinPage(file, newRootPid, true);
		}
	}

	return;
}


void BTreeIndex::showLeavesKey()
{

	const int lowVal = 500;
	const int highVal = 99999;
	
	startScan(static_cast<const void*> (& lowVal), Operator::GT, static_cast<const void*> (& highVal), Operator::LT);

	bool end = false;
	while(!end)
	{
		RecordId id;
		try {
			scanNext(id);
		} catch (IndexScanCompletedException)
		{
			end = true;
		}
	}
	endScan();
}

}


