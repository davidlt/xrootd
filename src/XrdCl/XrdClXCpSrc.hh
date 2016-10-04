/*
 * XrdClXCpSrc.hh
 *
 *  Created on: Nov 22, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDCL_XRDCLXCPSRC_HH_
#define SRC_XRDCL_XRDCLXCPSRC_HH_


#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClSharedQueue.hh"
#include "XrdSys/XrdSysPthread.hh"

#include <map>
#include <vector>

namespace XrdCl
{

class XCpSrc
{
  public:

    /**
     * Constructor.
     *
     * @param url       : source URL
     * @param chunkSize : chunk size
     * @param parallel  : number of parallel chunks
     * @param sink      : the sink where all the
     *                    chunks go
     */
    XCpSrc( const std::string &url, uint32_t chunkSize, uint8_t parallel, SharedQueue<ChunkInfo> *sink ) :
      pStatus( stOK, suDone ), pUrl( url ), pFile( new File() ), pSize( -1 ), pCurrentOffset( 0 ), pBlkEnd( 0 ),
      pChunkSize( chunkSize ), pParallel( parallel ), pDataTransfered( 0 ), pSink( sink->Self() ), pRefCount( 1 ) { }

    /**
     * Deletes the instance if the reference counter reached 0.
     */
    void Delete()
    {
      XrdSysMutexHelper lck( pMtx );
      --pRefCount;
      if( !pRefCount )
        delete this;
    }

    /**
     * Increments the reference counter.
     *
     * @return : myself.
     */
    XCpSrc* Self()
    {
      XrdSysMutexHelper lck( pMtx );
      ++pRefCount;
      return this;
    }

    /**
     * Initialize the source.
     *
     * @param fileSize : assumed file size, if value is
     *                   negative a stat is performed
     *
     * @return         : the status of the operation
     */
    XRootDStatus Initialize( int64_t fileSize = -1 );

    /**
     * Spawns new asynchronous chunk transfers.
     * First stolen chunks are being tried,
     * afterwards the source continues with its
     * own block.
     */
    XRootDStatus ReadChunk();

    /**
     * Report a read result. If the read failed the status of
     * the source is being set to error.
     *
     * @param status : status of the read operation
     * @param chunk  : the chunk read, if the operation
     *                 was successful
     */
    void ReportResult( XRootDStatus *status, ChunkInfo *chunk );

    /**
     * Allocates new block to the source. Also, sets the
     * source status to suContinue.
     *
     * @param offset : offset of the block
     * @param size   : block size
     */
    void SetBlock( uint64_t offset, uint64_t size );

    /**
     * Get file size.
     *
     * @return : file size
     */
    int64_t GetSize()
    {
      return pSize;
    }

    /**
     * Get source status.
     */
    XRootDStatus GetStatus()
    {
      XrdSysMutexHelper lck( pMtx );
      return pStatus;
    }

    /**
     * Steal load from given source:
     * 1. if the status of the source is suDone there's nothing to be done
     * 2. otherwise, if the status of the source is Err than take over its work (all of it)
     * 3. otherwise, if there is still a block of data to transfer, steal respective fraction
     * 4. otherwise,if there are stolen chunks, steal them
     * 5. otherwise steal the ongoing work
     *
     * @param src : the source from whom we are stealing
     *
     * @return    : true if there is a possibility of
     *              duplicate chunk downloads, false
     *              otherwise
     */
    bool Steal( XCpSrc *src );

    /**
     * Check if we still have work to do, this includes:
     * - a block to download
     * - stolen chunks
     * - ongoing chunks
     */
    bool HasWork()
    {
      XrdSysMutexHelper lck( pMtx );
      return uint64_t( pCurrentOffset ) < pBlkEnd || !pOngoing.empty() || !pStolen.empty();
    }

    /**
     * Check if we still have a block of size greater than 0.
     */
    bool HasBlock()
    {
      XrdSysMutexHelper lck( pMtx );
      return uint64_t( pCurrentOffset ) < pBlkEnd;
    }

    /**
     * Get the efficiency calculator.
     *
     * (greater indicator means lower efficiency!)
     */
    double EfficiencyIndicator();

  private:

    /**
     * Delete status and chunk info objects.
     */
    static void Delete( XRootDStatus *status, ChunkInfo *chunk )
    {
      delete status;
      if( chunk )
      {
        delete[] static_cast<char*>( chunk->buffer );
        delete   chunk;
      }
    }

    /**
     * Destructor.
     *
     * Private! Use Delete to destroy the object!
     */
    virtual ~XCpSrc()
    {
      pSink->Delete();
      delete pFile;
    }

    /**
     * Source status.
     */
    XRootDStatus                pStatus;

    /**
     * Source URL.
     */
    const std::string           pUrl;

    /**
     * Handle to the file.
     */
    XrdCl::File                *pFile;

    /**
     * File size.
     */
    int64_t                     pSize;

    /**
     * The offset of the next chunk to be transfered.
     */
    uint64_t                    pCurrentOffset;

    /**
     * End of the our block.
     */
    uint64_t                    pBlkEnd;

    /**
     * Chunk size.
     */
    uint32_t                    pChunkSize;

    /**
     * Number of parallel chunks.
     */
    uint8_t                     pParallel;

    /**
     * Total number of data transfered from this source.
     */
    uint64_t                    pDataTransfered;

    /**
     * The sink where all the transfered chunks go.
     */
    SharedQueue<ChunkInfo>     *pSink;

    /**
     * A map of ongoing transfers (the offset is the key,
     * the chunk size is the value).
     */
    std::map<uint64_t, uint64_t>  pOngoing;

    /**
     * A map of stolen chunks (again the offset is the key,
     * the chunk size is the value).
     */
    std::map<uint64_t, uint64_t>  pStolen;

    /**
     * Instance mutex.
     */
    mutable XrdSysRecMutex        pMtx;

    /**
     * Reference counter.
     */
    uint8_t                       pRefCount;
};

} /* namespace XrdCl */

#endif /* SRC_XRDCL_XRDCLXCPSRC_HH_ */
