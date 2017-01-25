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
  delete[] bufDescTable;
  delete[] bufPool;
  delete[] hashTable;
}

void BufMgr::advanceClock()
{
  clockHand = (clockHand + 1) % numBufs;
  std::cout << "clockHand = " << clockHand << ", numBufs = " << numBufs << std::endl;
}

void BufMgr::allocBuf(FrameId & frame) 
{
  int iteration = 0;
  FrameId initClockhand = clockHand;
  // advanceClock(clockHand);
  std::cout << "clockHand = " << clockHand << ", numBufs = " << numBufs << std::endl;

  // Throws BufferExceededException if all buffer frames are pinned
  while(iteration < 3)
  {
    std::cout << "in allocBuf iteration = " << iteration << std::endl;
    assert(clockHand >= 0);
    assert(clockHand < numBufs);

    if(clockHand == initClockhand)
      ++iteration;

    if(!bufDescTable[clockHand].valid)
    {
      std::cout << "invalid buf clockHand = " << clockHand << std::endl;
      frame = clockHand;
      return;
    }

    std::cout << "after invalid buf clockHand = " << clockHand << std::endl;

    if(bufDescTable[clockHand].pinCnt != 0)
    {
      advanceClock();
      continue;
    }

    if(bufDescTable[clockHand].refbit)
    {
      bufDescTable[clockHand].refbit = false;
      advanceClock();
      continue;
    }

    if(bufDescTable[clockHand].dirty)
      bufDescTable[clockHand].file->writePage(bufPool[clockHand]);

    // clear all related fields and return the frame
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
    bufDescTable[clockHand].Clear();
    frame = clockHand;
  }

  // no replaceable frame found
  throw BufferExceededException();
  
}
	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
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
    bufDescTable[targetFrameId].refbit = true;
    ++bufDescTable[targetFrameId].pinCnt;
    // page returned by reference
    page = &(bufPool[targetFrameId]);
    ++bufStats.accesses;
  }
  else
  {
    allocBuf(targetFrameId);
    bufPool[targetFrameId] = file->readPage(pageNo);
    hashTable->insert(file, pageNo, targetFrameId);
    bufDescTable[targetFrameId].Set(file, pageNo);
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
    }
  }
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page targetPage = file->allocatePage();
  pageNo = targetPage.page_number();
  FrameId targetFrameId = 0;
  allocBuf(targetFrameId);
  bufPool[targetFrameId] = targetPage;
  hashTable->insert(file, pageNo, targetFrameId);
  bufDescTable[targetFrameId].Set(file, pageNo);
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
