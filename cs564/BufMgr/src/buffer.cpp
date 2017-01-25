/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

#include <cassert>

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  for(unsigned i = 0; i < numBufs; ++i)
  {
    if(bufDescTable[i].dirty)
    {
      assert(bufDescTable[i].valid);
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  }
  delete[] bufDescTable;
  delete[] bufPool;
  // delete[] hashTable;
}

void BufMgr::advanceClock()
{
  clockHand = (clockHand + 1) % numBufs;

}

void BufMgr::allocBuf(FrameId & frame) 
{
  bool pinned[numBufs];

  for(unsigned i = 0; i < numBufs; ++i)
    pinned[i] = false;

  // Throws BufferExceededException if all buffer frames are pinned
  while(1)
  {
    advanceClock();

    bool end = true;
    for(unsigned i = 0; i < numBufs; ++i)
      end = end & pinned[i];
    if(end)
      break;

    if(!bufDescTable[clockHand].valid)
    {
      frame = clockHand;
      // bufPool[clockHand] = Page();
      return;
    }

    if(bufDescTable[clockHand].refbit)
    {
      bufDescTable[clockHand].refbit = false;
      continue;
    }

    if(bufDescTable[clockHand].pinCnt != 0)
    {
      pinned[clockHand] = true;
      continue;
    }


    assert(bufDescTable[clockHand].pinCnt == 0);
    assert(bufDescTable[clockHand].refbit == 0);

    if(bufDescTable[clockHand].dirty)
    {
      // std::cout << "write back dirty page " << bufDescTable[clockHand].pageNo << ", file = " << bufDescTable[clockHand].file->filename() << std::endl;
      bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
    }

    // clear all related fields and return the frame
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    bufDescTable[clockHand].Clear();
    // bufPool[clockHand] = Page();
    frame = clockHand;
    return;
  }

  // no replaceable frame found
  throw BufferExceededException();
  
}
	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  // std::cout << "In readPage file: " << file->filename() << ", pageNo = " << pageNo << std::endl;
  FrameId targetFrameId = 0;
  bool noHash = false;
  try
  {
    hashTable->lookup(file, pageNo, targetFrameId);
  }
  catch(HashNotFoundException)
  {
    noHash = true;
  }

  if(!noHash)
  {
    // std::cout << "Hased in read page, targetFrameId = " << targetFrameId << std::endl;
    bufDescTable[targetFrameId].refbit = true;
    ++bufDescTable[targetFrameId].pinCnt;
    // page returned by reference
    page = &(bufPool[targetFrameId]);
    // ++bufStats.accesses;
  }
  else
  {
    // std::cout << "Not Hased in read page" << std::endl;
    allocBuf(targetFrameId);
    bufPool[targetFrameId] = file->readPage(pageNo);
    // std::cout << "targetFrameId = " << targetFrameId << std::endl;
    bufDescTable[targetFrameId].Set(file, pageNo);
    hashTable->insert(file, pageNo, targetFrameId);
    // page returned by reference
    page = &(bufPool[targetFrameId]);
  }

  return;
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId targetFrameId = 0;
  try
  {
    hashTable->lookup(file, pageNo, targetFrameId);
  }
  catch(HashNotFoundException)
  {
    // do nothing if hash not found
    return;
  }

  if(bufDescTable[targetFrameId].pinCnt == 0)
    throw PageNotPinnedException(file->filename(), pageNo, targetFrameId);

  if(dirty == true)
      bufDescTable[targetFrameId].dirty = dirty;
  --bufDescTable[targetFrameId].pinCnt;
}

void BufMgr::flushFile(const File* file) 
{
  for(unsigned i = 0; i < numBufs; ++i)
  {
    if(bufDescTable[i].file == file)
    {
      PageId pageNo = bufDescTable[i].pageNo;
      FrameId targetFrameId = bufDescTable[i].frameNo;
      if(!bufDescTable[i].valid)
        throw BadBufferException(targetFrameId, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);

      if(bufDescTable[i].pinCnt != 0)
        throw PagePinnedException(file->filename(), pageNo, targetFrameId);

      if(bufDescTable[i].dirty)
        bufDescTable[i].file->writePage(bufPool[i]);

      // clear all related fields
      hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
      bufDescTable[i].Clear();
      // bufPool[i]=Page();
    }
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page targetPage = file->allocatePage();
  pageNo = targetPage.page_number();
  FrameId targetFrameId = 0;
  allocBuf(targetFrameId);
  bufDescTable[targetFrameId].Set(file, pageNo);
  // bufDescTable[targetFrameId].dirty = true;
  bufPool[targetFrameId] = targetPage;
  hashTable->insert(file, pageNo, targetFrameId);
  page = &(bufPool[targetFrameId]);

  ++bufStats.accesses;
}

void BufMgr::disposePage(File* file, const PageId pageNo)
{
  FrameId targetFrameId = 0;
  bool noHash = false;
  try
  {
    hashTable->lookup(file, pageNo, targetFrameId);
  }
  catch(HashNotFoundException)
  {
    noHash = true;  
  }

  if(!noHash)
  {
    bufDescTable[targetFrameId].Clear();
    hashTable->remove(file, pageNo);
    // bufPool[targetFrameId] = Page();
  }

  // remove the page from file
  file->deletePage(pageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
