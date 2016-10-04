/*
 * XrdClXCpCtx.hh
 *
 *  Created on: Nov 22, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDCL_XRDCLXCPCTX_HH_
#define SRC_XRDCL_XRDCLXCPCTX_HH_

#include "XrdCl/XrdClXCpSrc.hh"
#include "XrdCl/XrdClSharedQueue.hh"

#include <list>
#include <queue>
#include <set>

namespace XrdCl
{

class XCpCtx
{
  public:

    /**
     * Constructor.
     *
     * @param urls        : replicas
     * @param blockSize   : the size of the block allocated to a single  source
     * @param parallelSrc : number of parallel sources
     * @param chunkSize   : chunk size
     * @ parallelChunks   : number of parallel chunks (per source)
     */
    XCpCtx( const std::vector<std::string> &urls, uint64_t blockSize, uint64_t parallelSrc, uint64_t chunkSize, uint64_t parallelChunks );

    /**
     * Destructor.
     */
    virtual ~XCpCtx();

    // initialize the file object
    /**
     * Initialize the extreme copy context.
     *
     * @param fileSize : the assumed file size if value is positive
     *
     * @return : operation status
     */
    XRootDStatus Initialize( int64_t fileSize = -1 );

    /**
     * @return : file size
     */
    virtual int64_t GetSize();

    /**
     * Gets the next chunk that has been transfered.
     *
     * @param ci : the received chunk
     *
     * @return   : operation status
     *             suDone     - all chunks have been transfered
     *             suContinue - continue, ci contains the next
     *                          chunk
     *             suRetry    - continue, ci does not contain
     *                          the next chunk
     */
    virtual XRootDStatus GetChunk( XrdCl::ChunkInfo &ci );

  private:

    /**
     * Delete ChunkInfo object.
     */
    static void DeleteChunkInfo( ChunkInfo* chunk );

    /**
     * Move sources in error state to failed queue
     */
    void RemoveFailed();

    /**
     * If number of initialized sources is less than
     * the maximum number of allowed parallel sources,
     * creates and initializes new sources.
     *
     * @param fileSize : assumed file size if positive
     */
    void InitNewSrc( int64_t fileSize = -1 );

    /**
     * Allocate new block for the given source:
     *
     * 1. allocate new block, if there are blocks
     *    left
     * 2. otherwise, take over the load of a failed
     *    source
     * 3. otherwise, steal from a least efficient
     *    replica.
     *
     * @param src : the source that requires new load
     *
     * @return    : true if there is a possibility of
     *              duplicate chunk downloads, false
     *              otherwise
     */
    bool AllocBlock( XCpSrc *src );

    /**
     * @return : number of sources that are in suDone
     *           state
     */
    size_t AcumulateDone();

    /**
     * Returns the least efficient source
     *
     * @param exclude : the source that will be omitted
     *                  in the search
     *
     * @return        : the weakest link
     */
    XCpSrc* WeakestLink( XCpSrc *exclude );

    /**
     * The URLs of all the replicas that were provided
     * to us.
     */
    std::queue<std::string>    pUrls;

    /**
     * The size of the block allocated to a single  source.
     */
    uint64_t                   pBlockSize;

    /**
     * Number of parallel sources.
     */
    uint8_t                    pParallelSrc;

    /**
     * Chunk size.
     */
    uint32_t                   pChunkSize;

    /**
     * Number of parallel chunks per source.
     */
    uint8_t                    pParallelChunks;

    /**
     * Offset in the file (everything before the offset
     * has been allocated, everything after the offset
     * needs to be allocated)
     */
    uint64_t                   pOffset;

    /**
     * File size.
     */
    uint64_t                    pSize;

    /**
     * Active sources.
     */
    std::list<XCpSrc*>         pSources;

    /**
     * Queue of failed sources.
     */
    std::queue<XCpSrc*>        pFailed;

    /**
     * A queue shared between all the sources (producers),
     * and the extreme copy context (consumer).
     */
    SharedQueue<ChunkInfo>    *pSink;

    /**
     * A flag denoting whether duplicate chunk downloads
     * are possible (this can only happen at the very end
     * when a source may steal ongoing chunks).
     */
    bool                       pBewareDuplicates;

    /**
     * Set of received chunks, only in use when pBewareDuplicates
     * equals to true.
     */
    std::set<uint64_t>         pReceived;

};

} /* namespace XrdCl */

#endif /* SRC_XRDCL_XRDCLXCPCTX_HH_ */
