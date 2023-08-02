// Author: Enrico Guiraud CERN 04/2023

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RBULKSTATICARRAYREADER
#define ROOT_RDF_RBULKSTATICARRAYREADER

#include <ROOT/RDF/RBulkReaderBase.hxx>
#include <ROOT/RVec.hxx>
#include <TTree.h>
#include <TFile.h>

namespace ROOT {
namespace Internal {
namespace RDF {

template <typename ColType>
class RBulkStaticArrayReader final : public RBulkReaderBase {
   using Element_t = typename ColType::value_type;
   ROOT::RVec<Element_t> fFlattenedElements;
   /// Each inner RVec is a view over the data in fFlattenedElements.
   ROOT::RVec<ROOT::RVec<Element_t>> fArrays;
   std::size_t fStaticArraySize;

public:
   RBulkStaticArrayReader(TBranch &branch, TTree &tree, std::size_t staticSize, std::size_t maxEventsPerBulk)
      : RBulkReaderBase(branch, tree), fFlattenedElements(maxEventsPerBulk * staticSize), fStaticArraySize(staticSize)
   {
      static_assert(VecOps::IsRVec<ColType>::value,
                    "Something went wrong, RBulkStaticArrayReader should only be used for RVec columns.");

      // point fArrays to the data in fFlattenedElements
      fArrays.reserve(maxEventsPerBulk);
      for (std::size_t i = 0u; i < maxEventsPerBulk; ++i)
         fArrays.emplace_back(&fFlattenedElements[i * fStaticArraySize], fStaticArraySize);
   }

   // here arrayOffset and branchBulkOffset are counted in entries, not in array elements
   void LoadN(std::size_t N, std::size_t arrayOffset, std::size_t branchBulkOffset)
   {
      // work on raw pointers to side-step RVec::operator[] which is slightly more costly than simple pointer arithmetic
      Element_t *elementsCache = fFlattenedElements.data() + arrayOffset * fStaticArraySize;

      char *data = reinterpret_cast<char *>(fBuf.GetCurrent());
      // advance to the first element we are interested in
      data += branchBulkOffset * sizeof(Element_t) * fStaticArraySize;

      // copy the new elements in fFlattenedElements
      for (std::size_t i = 0u; i < N * fStaticArraySize; ++i)
         frombuf(data, elementsCache + i); // `frombuf` also advances the `data` pointer
   }

   void *LoadImpl(const Internal::RDF::RMaskedEntryRange &requestedMask, std::size_t bulkSize) final
   {
      if (requestedMask.FirstEntry() == fLoadedEntriesBegin)
         return fArrays.data(); // the requested bulk is already loaded, nothing to do

      fLoadedEntriesBegin = requestedMask.FirstEntry();
      std::size_t nLoaded = 0u;

      if (fBranchBulkSize > 0u) { // we have a branch bulk loaded
         const std::size_t nAvailable = (fBranchBulkBegin + fBranchBulkSize) - fLoadedEntriesBegin;
         const auto nToLoad = std::min(nAvailable, bulkSize);
         LoadN(nToLoad, /*arrayOffset*/ 0u, /*branchBulkOffset*/ fLoadedEntriesBegin - fBranchBulkBegin);
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
               "RBulkStaticArrayReader: could not load branch values. This should never happen. File name: " +
               std::string(fBranch->GetTree()->GetCurrentFile()->GetName()) +
               ". File title: " + std::string(fBranch->GetTree()->GetCurrentFile()->GetTitle()) +
               ". Branch name: " + std::string(fBranch->GetFullName()) +
               ". Requested entry at beginning of bulk: " + std::to_string(basketStart));

         fBranchBulkSize = ret;
         fBranchBulkBegin = basketStart + fChainOffset;
         const auto nToLoad = std::min(fBranchBulkSize - nToSkip, bulkSize - nLoaded);
         LoadN(nToLoad, /*arrayOffset*/ nLoaded, /*branchBulkOffset*/ nToSkip);
         nLoaded += nToLoad;
      }

      return fArrays.data();
   }
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT
#endif
