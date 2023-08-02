// Author: Enrico Guiraud CERN 04/2023

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RBULKDYNAMICARRAYREADER
#define ROOT_RDF_RBULKDYNAMICARRAYREADER

#include <ROOT/RDF/RBulkReaderBase.hxx>
#include <ROOT/RVec.hxx>
#include <TTree.h>
#include <TFile.h>

namespace ROOT {
namespace Internal {
namespace RDF {

template <typename ColType, typename SizeType>
class RBulkDynamicArrayReader final : public RBulkReaderBase {
   using Element_t = typename ColType::value_type;
   ROOT::RVec<Element_t> fFlattenedElements;
   /// Each inner RVec is a view over the data in fFlattenedElements.
   ROOT::RVec<ROOT::RVec<Element_t>> fArrays;
   /// Pointer to the column reader for the size of the dynamic arrays. Never null.
   RColumnReaderBase *fSizeReader;
   /// Index of the element in the branch bulk after the last one we already read.
   std::size_t fBranchBulkElementsOffset = 0u;

   // Copy N entries from fBuf to fFlattenedElements, make fArrays point to them.
   // Expects fBuf to have enough values available and fFlattenedElements to have enough room for them.
   // Return the number of total array elements loaded.
   std::size_t LoadN(std::size_t N, std::size_t arrayOffset, std::size_t elementsOffset, SizeType *sizes)
   {
      // work on raw pointers to side-step RVec::operator[] which is slightly more costly than simple pointer arithmetic
      Element_t *elementsCache = fFlattenedElements.data() + elementsOffset;
      ROOT::RVec<Element_t> *arrayCache = fArrays.data() + arrayOffset;

      char *data = reinterpret_cast<char *>(fBuf.GetCurrent());
      data += fBranchBulkElementsOffset * sizeof(Element_t); // advance to the first element we are interested in

      std::size_t nElements = 0u;

      // make the RVecs in fArrays point to the right addresses in fFlattenedElements
      for (std::size_t i = 0u; i < N; ++i) {
         const std::size_t size = sizes[arrayOffset];
         ROOT::Internal::VecOps::ResetView(*arrayCache, elementsCache + nElements, size);
         nElements += size;
         ++arrayOffset;
         ++arrayCache;
      }

      // copy the new elements in fFlattenedElements
      for (std::size_t i = 0u; i < nElements; ++i)
         frombuf(data, elementsCache + i); // `frombuf` also advances the `data` pointer

      fBranchBulkElementsOffset += nElements;
      return nElements;
   }

public:
   RBulkDynamicArrayReader(TBranch &branch, TTree &tree, RColumnReaderBase &sizeReader, std::size_t maxEventsPerBulk)
      : RBulkReaderBase(branch, tree), fSizeReader(&sizeReader)
   {
      static_assert(VecOps::IsRVec<ColType>::value,
                    "Something went wrong, RBulkDynamicArrayReader should only be used for RVec columns.");

      // Put all fArrays in non-owning mode (aka view mode), so later we can just call ResetView on them.
      fFlattenedElements.resize(1ull);
      fArrays.reserve(maxEventsPerBulk);
      for (std::size_t i = 0u; i < maxEventsPerBulk; ++i)
         fArrays.emplace_back(fFlattenedElements.data(), 1u);
   }

   void *LoadImpl(const Internal::RDF::RMaskedEntryRange &requestedMask, std::size_t bulkSize) final
   {
      if (requestedMask.FirstEntry() == fLoadedEntriesBegin) // the requested bulk is already loaded, nothing to do
         return fArrays.data();

      // Load array sizes.
      // We actually need all values, not just the ones from the `requestedMask`, because for branches that support bulk
      // reading we always read all values ignoring the mask. The assumption here is that size leafs of bulk branches
      // always support bulk themselves, so we can just pass the same `requestedMask` here and it will be ignored,
      // just like we are going to ignore it here.
      auto *sizes = static_cast<SizeType *>(fSizeReader->Load(requestedMask, bulkSize));

      // Make sure we have enough space in the array elements cache
      const auto nTotalElements = std::accumulate(sizes, sizes + bulkSize, std::size_t(0u));
      fFlattenedElements.resize(nTotalElements);

      fLoadedEntriesBegin = requestedMask.FirstEntry();
      std::size_t nLoadedEntries = 0u;
      // Offset into fFlattenedElements, points to where new elements should be inserted.
      std::size_t elementsOffset = 0u;

      if (fBranchBulkSize > 0u) { // we have a branch bulk loaded
         const std::size_t nAvailable = (fBranchBulkBegin + fBranchBulkSize) - fLoadedEntriesBegin;
         const auto nToLoad = std::min(nAvailable, bulkSize);
         elementsOffset += LoadN(nToLoad, /*arrayOffset*/ nLoadedEntries, elementsOffset, sizes);
         nLoadedEntries += nToLoad;
      }

      while (nLoadedEntries < bulkSize) {
         // assert we either have not loaded a bulk yet or we exhausted the last branch bulk
         assert(fBranchBulkSize == 0u || fBranchBulkBegin + fBranchBulkSize == fLoadedEntriesBegin + nLoadedEntries);

         // It can happen that because of a TEntryList or a RDatasetSpec we start reading from the middle of a basket (i.e. not
         // from the beginning of a new cluster). If that's the case we need to adjust things so we read from the start of the basket:
         // GetEntriesSerialized does not allow reading a bulk starting from the middle of a basket.
         const Long64_t localEntry = fLoadedEntriesBegin + nLoadedEntries - fChainOffset;
         const Long64_t basketStartIdx =
            TMath::BinarySearch(fBranch->GetWriteBasket() + 1, fBranch->GetBasketEntry(), localEntry);
         const Long64_t basketStart = fBranch->GetBasketEntry()[basketStartIdx];
         const std::size_t nEntriesToSkip = localEntry - basketStart;

         // read in new branch bulk
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
         fBranchBulkElementsOffset = std::accumulate(sizes + nLoadedEntries, sizes + nLoadedEntries + nEntriesToSkip, std::size_t(0u));
         const auto nToLoad = std::min(fBranchBulkSize - nEntriesToSkip, bulkSize - nLoadedEntries);
         elementsOffset += LoadN(nToLoad, /*arrayOffset*/ nLoadedEntries, elementsOffset, sizes);
         nLoadedEntries += nToLoad;
      }

      return fArrays.data();
   }
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT
#endif
