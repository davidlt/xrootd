/*
 * XrdClSharedQueue.hh
 *
 *  Created on: Nov 22, 2016
 *      Author: simonm
 */

#ifndef SRC_XRDCL_XRDCLSHAREDQUEUE_HH_
#define SRC_XRDCL_XRDCLSHAREDQUEUE_HH_

#include "XrdCl/XrdClSyncQueue.hh"

namespace XrdCl
{

template <typename Item>
class SharedQueue : public SyncQueue<Item*>
{
  public:

    typedef void (*Dealloc_t)( Item* );

    SharedQueue( Dealloc_t dealloc = 0 ) : Dealloc( dealloc ), pRefCount( 1 ) { }

    void Delete()
    {
      XrdSysMutexHelper lck( this->pMutex );
      --pRefCount;
      if( !pRefCount )
        delete this;
    }

    SharedQueue* Self()
    {
      XrdSysMutexHelper lck( this->pMutex );
      ++pRefCount;
      return this;
    }

    bool Empty()
    {
      XrdSysMutexHelper lck( this->pMutex );
      return this->pQueue.empty();
    }

    size_t Size()
    {
      XrdSysMutexHelper lck( this->pMutex );
      return this->pQueue.size();
    }

  private:

    ~SharedQueue()
    {
      if( !Dealloc ) return;

      while( !this->pQueue.empty() )
      {
        Item *item = this->pQueue.front();
        this->pQueue.pop();
        Dealloc( item );
      }
    }

    Dealloc_t Dealloc;

//    mutable XrdSysMutex pMutex;
    uint8_t             pRefCount;
};

}



#endif /* SRC_XRDCL_XRDCLSHAREDQUEUE_HH_ */
