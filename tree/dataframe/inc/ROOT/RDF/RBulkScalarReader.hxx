// Author: Enrico Guiraud CERN 04/2023

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RBULKSCALARREADER
#define ROOT_RDF_RBULKSCALARREADER

#include "RtypesCore.h"
#include "TMathBase.h"
#include <ROOT/RDF/RBulkReaderBase.hxx>
#include <ROOT/RVec.hxx>
#include <TTree.h>
#include <TFile.h>

#include <cassert>

namespace ROOT {
namespace Internal {
namespace RDF {

template <typename T>
class RBulkScalarReader final : public RBulkReaderBase {
   ROOT::RVec<T> fCachedValues;

   // Copy N entries starting from fBuf to fCachedValues, indexing them using bufOffset and cacheOffset respectively.
   // Expects fBuf to have enough values available and fCachedValues to have enough room for them.
   void LoadN(std::size_t N, std::size_t cacheOffset, std::size_t bufOffset)
   {
      char *data = reinterpret_cast<char *>(fBuf.GetCurrent());
      data += bufOffset * sizeof(T); // advance to the first element we are interested in

      T* elementsCache = fCachedValues.data() + cacheOffset;

      for (std::size_t i = 0u; i < N; ++i)
         frombuf(data, elementsCache + i); // `frombuf` also advances the `data` pointer
   }

public:
   RBulkScalarReader(TBranch &branch, TTree &tree, std::size_t maxEventsPerBulk)
      : RBulkReaderBase(branch, tree), fCachedValues(maxEventsPerBulk)
   {
   }

   void *LoadImpl(const Internal::RDF::RMaskedEntryRange &requestedMask, std::size_t bulkSize) final
   {
      if (requestedMask.FirstEntry() == fLoadedEntriesBegin)
         return &fCachedValues[0]; // the requested bulk is already loaded, nothing to do

      fLoadedEntriesBegin = requestedMask.FirstEntry();
      std::size_t nLoaded = 0u;

      if (fBranchBulkSize > 0u) { // we have a branch bulk loaded
         const std::size_t nAvailable = (fBranchBulkBegin + fBranchBulkSize) - fLoadedEntriesBegin;
         const auto nToLoad = std::min(nAvailable, bulkSize);
         LoadN(nToLoad, 0u, fLoadedEntriesBegin - fBranchBulkBegin);
         nLoaded += nToLoad;
      }

      while (nLoaded < bulkSize) {
         // assert we either have not loaded a bulk yet or we exhausted the last branch bulk
         assert(fBranchBulkSize == 0u || fBranchBulkBegin + fBranchBulkSize == fLoadedEntriesBegin + nLoaded);

         // It can happen that because of a TEntryList or a RDatasetSpec we start reading from the middle of a basket (i.e. not
         // from the beginning of a new cluster). If that's the case we need to adjust things so we read from the start of the basket:
         // GetEntriesSerialized does not allow reading a bulk starting from the middle of a basket.
         const Long64_t localEntry = fLoadedEntriesBegin + nLoaded - fChainOffset;
         const Long64_t basketStartIdx =
            TMath::BinarySearch(fBranch->GetWriteBasket() + 1, fBranch->GetBasketEntry(), localEntry);
         const Long64_t basketStart = fBranch->GetBasketEntry()[basketStartIdx];
         const std::size_t nToSkip = localEntry - basketStart;

         // GetEntriesSerialized does not byte-swap, so later we use frombuf to byte-swap and copy in a single pass
         const auto ret = fBranch->GetBulkRead().GetEntriesSerialized(basketStart, fBuf);
         if (ret == -1)
            throw std::runtime_error(
               "RBulkScalarReader: could not load branch values. This should never happen. File name: " +
               std::string(fBranch->GetTree()->GetCurrentFile()->GetName()) +
               ". File title: " + std::string(fBranch->GetTree()->GetCurrentFile()->GetTitle()) +
               ". Branch name: " + std::string(fBranch->GetFullName()) +
               ". Requested entry at beginning of bulk: " + std::to_string(basketStart));
         fBranchBulkSize = ret;
         fBranchBulkBegin = basketStart + fChainOffset;
         const auto nToLoad = std::min(fBranchBulkSize - nToSkip, bulkSize - nLoaded);
         LoadN(nToLoad, /*cacheOffset*/ nLoaded, /*bufOffset*/ nToSkip);
         nLoaded += nToLoad;
      }

      return &fCachedValues[0];
   }
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT
#endif
