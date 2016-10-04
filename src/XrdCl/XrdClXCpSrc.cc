/*
 * XrdClXCpSrc.cc
 *
 *  Created on: Nov 22, 2016
 *      Author: simonm
 */

#include "XrdCl/XrdClXCpSrc.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClConstants.hh"

namespace XrdCl
{

class ChunkHandler: public ResponseHandler
{
  public:

    ChunkHandler( XCpSrc* src, uint64_t offset, uint64_t size, char *buffer ) :
      pSrc( src->Self() ), pOffset( offset ), pSize( size ), pBuffer( buffer ) { }

    virtual ~ChunkHandler()
    {
      pSrc->Delete();
    }

    virtual void HandleResponse( XRootDStatus *status, AnyObject *response )
    {
      ChunkInfo *chunk = 0;
      if( response ) // get the response
      {
        response->Get( chunk );
        response->Set( ( int* )0 );
        delete response;
      }

      if( !chunk && status->IsOK() ) // if the response is not there make sure the status is error
      {
        *status = XRootDStatus( stError, errInternal );
      }

      if( !status->IsOK() )
      {
        delete[] pBuffer;
        delete   chunk;
        chunk = 0;
      }

      pSrc->ReportResult( status, chunk );

      delete this;
    }

  private:

    XCpSrc            *pSrc;
    uint64_t           pOffset;
    uint64_t           pSize;
    char              *pBuffer;
};

XRootDStatus XCpSrc::Initialize( int64_t fileSize )
{
  Log *log = DefaultEnv::GetLog();
  log->Debug( UtilityMsg, "Opening %s for reading",
                          pUrl.c_str() );

  std::string value;
  DefaultEnv::GetEnv()->GetString( "ReadRecovery", value );
  pFile->SetProperty( "ReadRecovery", value );

  XRootDStatus st = pFile->Open( pUrl, OpenFlags::Read );
  if( !st.IsOK() )
    return st;

  if( fileSize < 0 )
  {
    StatInfo *statInfo;
    st = pFile->Stat( false, statInfo );
    if( !st.IsOK() )
      return st;

    pSize = statInfo->GetSize();
    delete statInfo;
  }
  else
    pSize = fileSize;

  return XRootDStatus();
}

XRootDStatus XCpSrc::ReadChunk()
{
  if( !pFile->IsOpen() )
    pStatus = XRootDStatus( stError, errUninitialized );

  if( !pStatus.IsOK() ) return pStatus;

  while( pOngoing.size() < pParallel && !pStolen.empty() )
  {
    std::pair<uint64_t, uint64_t> p;
    { // synchronized section
      XrdSysMutexHelper lck( pMtx );
      std::map<uint64_t, uint64_t>::iterator itr = pStolen.begin();
      p = *itr;
      pOngoing.insert( p );
      pStolen.erase( itr );
    }

    char *buffer = new char[p.second];
    ChunkHandler *handler = new ChunkHandler( this, p.first, p.second, buffer );
    XRootDStatus st = pFile->Read( p.first, p.second, buffer, handler );
    if( !st.IsOK() )
    {
      delete[] buffer;
      delete   handler;
      ReportResult( new XRootDStatus( st ), 0 );
    }
  }

  while( pOngoing.size() < pParallel && pCurrentOffset < pBlkEnd )
  {
    uint64_t chunkSize = pChunkSize;
    if( pCurrentOffset + chunkSize > pBlkEnd )
      chunkSize = pBlkEnd - pCurrentOffset;

    { // synchronized section
      XrdSysMutexHelper lck( pMtx );
      pOngoing[pCurrentOffset] = chunkSize;
    }

    char *buffer = new char[chunkSize];
    ChunkHandler *handler = new ChunkHandler( this, pCurrentOffset, chunkSize, buffer );
    XRootDStatus st = pFile->Read( pCurrentOffset, chunkSize, buffer, handler );
    pCurrentOffset += chunkSize;
    if( !st.IsOK() )
    {
      delete[] buffer;
      delete   handler;
      ReportResult( new XRootDStatus( st ), 0 );
    }
  }

  { // synchronized section
    XrdSysMutexHelper lck( pMtx );
    if( pStatus.IsOK() )
    {
      if( pCurrentOffset < pBlkEnd || !pOngoing.empty() || !pStolen.empty() )
        pStatus = XRootDStatus( stOK, suContinue );
      else
        pStatus = XRootDStatus( stOK, suDone );
    }
  }

  return pStatus;
}

void XCpSrc::ReportResult( XRootDStatus *status, ChunkInfo *chunk )
{
  if( !status->IsOK() )
  {
    XrdSysMutexHelper lck( pMtx );
    pStatus = *status;
  }

  if( !pStatus.IsOK() )
  {
    Delete( status, chunk );
    pSink->Put( 0 );
    return;
  }

  XrdSysMutexHelper lck( pMtx );
  pOngoing.erase( chunk->offset );
  pDataTransfered += chunk->length;
  delete status;

  pSink->Put( chunk );
}

void XCpSrc::SetBlock( uint64_t offset, uint64_t size )
{
  XrdSysMutexHelper lck( pMtx );
  pCurrentOffset = offset;
  pBlkEnd        = offset + size;
  // make sure status is not 'Done'
  pStatus        = XRootDStatus( stOK, suContinue );
}

bool XCpSrc::Steal( XCpSrc *src )
{
  if( !src ) return false;

  XrdSysMutexHelper lck1( pMtx ), lck2( src->pMtx );

  if( src->pStatus.IsOK() && src->pStatus.code == suDone ) return false;

  if( !src->pStatus.IsOK() )
  {
    // the source we are stealing from is in error state, we can have everything

    pStolen.insert( src->pOngoing.begin(), src->pOngoing.end() );
    pStolen.insert( src->pStolen.begin(), src->pStolen.end() );
    pCurrentOffset = src->pCurrentOffset;
    pBlkEnd        = src->pBlkEnd;

    src->pOngoing.clear();
    src->pStolen.clear();
    src->pCurrentOffset = 0;
    src->pBlkEnd = 0;

    return false;
  }

  // the source we are stealing from is just slower, only take part of its work

  if( src->pCurrentOffset < src->pBlkEnd )
  {
    // the source still has a block of data
    uint64_t steal   = 0;
    uint64_t blkSize = src->pBlkEnd - src->pCurrentOffset;
    if( blkSize - steal <= pChunkSize )
    {
      steal = blkSize;
    }
    else
    {
      // the fraction of the block we want for ourselves
      double fraction = double( pDataTransfered ) / double( pDataTransfered + src->pDataTransfered );
      steal = fraction * blkSize;
    }

    pCurrentOffset = src->pBlkEnd - steal;
    pBlkEnd        = src->pBlkEnd;
    src->pBlkEnd  -= steal;

    return false;
  }

  if( !src->pStolen.empty() )
  {
    pStolen.insert( src->pStolen.begin(), src->pStolen.end() );
    src->pStolen.clear();

    return false;
  }

  if( pDataTransfered > src->pDataTransfered )
  {
    // the source only has ongoing transfers
    pStolen.insert( src->pOngoing.begin(), src->pOngoing.end() );
    return true;
  }

  return false;
}

double XCpSrc::EfficiencyIndicator()
{
  XrdSysMutexHelper lck( pMtx );
  double toBeTransfered = 0; // bytes to be transfered
  std::map<uint64_t, uint64_t>::iterator itr;

  for( itr = pOngoing.begin() ; itr != pOngoing.end() ; ++itr )
    toBeTransfered += itr->second;

  for( itr = pStolen.begin() ; itr != pStolen.end() ; ++itr )
    toBeTransfered += itr->second;

  toBeTransfered += pBlkEnd - pCurrentOffset;

  return toBeTransfered / pDataTransfered;
}



} /* namespace XrdCl */
