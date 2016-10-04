/*
 * XrdClXCpCtx.cc
 *
 *  Created on: Nov 22, 2016
 *      Author: simonm
 */

#include "XrdClXCpCtx.hh"

#include <deque>

namespace XrdCl
{

XCpCtx::XCpCtx( const std::vector<std::string> &urls, uint64_t blockSize, uint64_t parallelSrc,
    uint64_t chunkSize, uint64_t parallelChunks ) :
  pUrls( std::deque<std::string>( urls.begin(), urls.end() ) ), pBlockSize( blockSize ),
  pParallelSrc( parallelSrc ), pChunkSize( chunkSize ), pParallelChunks( parallelChunks ),
  pOffset( 0 ), pSize( 0 ), pSink( new SharedQueue<ChunkInfo>( DeleteChunkInfo ) ),
  pBewareDuplicates( false )
{

}

XCpCtx::~XCpCtx()
{
  std::list<XCpSrc*>::iterator itr;
  for( itr = pSources.begin() ; itr != pSources.end(); ++itr )
  {
    XCpSrc *src = *itr;
    src->Delete();
  }

  while( !pFailed.empty() )
  {
    XCpSrc *failed = pFailed.front(); pFailed.pop();
    failed->Delete();
  }

  pSink->Delete();
}

XRootDStatus XCpCtx::Initialize( int64_t fileSize )
{
  InitNewSrc( fileSize );

  if( pSources.empty() )
    return XRootDStatus( stError, errInvalidRedirectURL ); // TODO which error code should we use (?)

  if( fileSize < 0 )
    pSize = pSources.front()->GetSize();
  else
    pSize = fileSize;

  // calculate how much data we can allocate per source
  uint64_t allocation = pSize / pSources.size();
  // if the allocation is smaller than the block size,
  // adjust the block size
  if( allocation < pBlockSize ) pBlockSize = allocation;
  // if the block size is smaller than the chunk size,
  // make it equal
  if( pBlockSize < pChunkSize ) pBlockSize = pChunkSize;

  // assign initial blocks to our sources
  std::list<XCpSrc*>::iterator itr;
  for( itr = pSources.begin() ; itr != pSources.end() ; ++itr )
  {
    XCpSrc *src = *itr;
    src->SetBlock( pOffset, pBlockSize );
    pOffset += pBlockSize;
    if( pOffset > pSize ) break;
  }

  return XRootDStatus();
}

int64_t XCpCtx::GetSize()
{
  return pSize;
}

XCpSrc* XCpCtx::WeakestLink( XCpSrc *exclude )
{
  double efficiencyIndicator = 0;
  XCpSrc *ret = 0;

  std::list<XCpSrc*>::iterator itr;
  for( itr = pSources.begin() ; itr != pSources.end() ; ++itr )
  {
    XCpSrc *src = *itr;
    if( src == exclude ) continue;
    double tmp = src->EfficiencyIndicator();
    if( tmp > efficiencyIndicator )
    {
      ret = src;
      efficiencyIndicator = tmp;
    }
  }

  return ret;
}

void XCpCtx::RemoveFailed()
{
  std::list<XCpSrc*>::iterator itr;
  for( itr = pSources.begin() ; itr != pSources.end() ; ++itr )
  {
    XCpSrc *src = *itr;
    if( !src->GetStatus().IsOK() )
    {
      pSources.erase( itr );
      if( src->HasWork() )
        pFailed.push( src );
      else
        src->Delete();
    }
  }
}

void XCpCtx::InitNewSrc( int64_t fileSize )
{
  while( pSources.size() < pParallelSrc && !pUrls.empty() )
  {
    // get next url
    std::string url = pUrls.front(); pUrls.pop();
    // try to create an extreme-copy source
    XCpSrc *src = new XCpSrc( url, pChunkSize, pParallelChunks, pSink );
    XRootDStatus st = src->Initialize( fileSize );
    if( st.IsOK() )
      pSources.push_back( src );
    else
      src->Delete();
  }
}

bool XCpCtx::AllocBlock( XCpSrc *src )
{
  // 1. just allocate new block
  if( pOffset < pSize )
  {
    uint64_t block = pBlockSize;
    if( pOffset + block > pSize ) block = pSize - pOffset;
    src->SetBlock( pOffset, block );
    pOffset += block;
    return false;
  }
  // 2. take over failed source
  if( !pFailed.empty() )
  {
    XCpSrc *failed = pFailed.front(); pFailed.pop();
    src->Steal( failed );
    failed->Delete();
    return false;
  }
  // 3. steal from the weakest link
  XCpSrc *wLink = WeakestLink( src );
  return src->Steal( wLink );
}

size_t XCpCtx::AcumulateDone()
{
  size_t done = 0;
  std::list<XCpSrc*>::iterator itr;

  for( itr = pSources.begin() ; itr != pSources.end() ; ++itr )
  {
    XCpSrc *src = *itr;
    XRootDStatus st = src->GetStatus();
    if( st.code == suDone ) ++done;
  }

  return done;
}

XRootDStatus XCpCtx::GetChunk( XrdCl::ChunkInfo &ci )
{
  // Move failed sources to 'pFailed' queue
  RemoveFailed();

  // create new sources if there is room
  InitNewSrc( pSize );

  // if there are no more sources at this point we can give up
  if( pSources.empty() ) return XRootDStatus( stError ); // TODO think about the error code

  // start new asynchronous transfers
  std::list<XCpSrc*>::iterator itr;
  for( itr = pSources.begin() ; itr != pSources.end() ; ++itr )
  {
    XCpSrc *src = *itr;
    XRootDStatus st = src->ReadChunk();
    // if the given source consumed already the whole block
    if( !src->HasBlock() )
      // remember if we need to care about duplicates
      // (this could happen only at the end for few last
      //  chunks)
      pBewareDuplicates |= AllocBlock( src );
  }

  // if all sources are done and the sink is empty we are done :-)
  if( AcumulateDone() == pSources.size() && pSink->Empty() )
    return XRootDStatus( stOK, suDone );

  ChunkInfo *chunk = pSink->Get();
  if( chunk )
  {
    if( pBewareDuplicates )
    {
      // if duplicates are possible, check if the chunk was already transfered
      std::pair< std::set<uint64_t>::iterator, bool > p;
      p = pReceived.insert( chunk->offset );
      // if yes, delete the chunk, and tell control to retry
      if( !p.second )
      {
        DeleteChunkInfo( chunk );
        return XRootDStatus( stOK, suRetry );
      }
    }
    ci = *chunk;
    return XRootDStatus( stOK, suContinue );
  }

  return XRootDStatus( stOK, suRetry );
}

void XCpCtx::DeleteChunkInfo( ChunkInfo* chunk )
{
  if( chunk )
  {
    delete[] static_cast<char*>( chunk->buffer );
    delete chunk;
  }
}

} /* namespace XrdCl */
