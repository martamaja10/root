// Author: Enrico Guiraud 09/2022

/*************************************************************************
 * Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RMASKEDENTRYRANGE
#define ROOT_RDF_RMASKEDENTRYRANGE

#include <Rtypes.h>
#include <ROOT/RVec.hxx>

#include <limits>

namespace ROOT {
namespace Internal {
namespace RDF {

/// RDataFrame's internal representation of an entry range with a boolean mask.
/// The mask has static size but depending on the dynamic bulk size fewer elements could be in use:
/// do not take the size of the mask as the size of the bulk.
class RMaskedEntryRange {
   ROOT::RVec<bool> fMask; ///< Boolean mask. Its size is set at construction time.
   Long64_t fBegin;        ///< Entry number of the first entry in the range this mask corresponds to.

public:
   RMaskedEntryRange(std::size_t size) : fMask(size, true), fBegin(-1ll) {}
   Long64_t FirstEntry() const { return fBegin; }
   const bool &operator[](std::size_t idx) const { return fMask[idx]; }
   bool &operator[](std::size_t idx) { return fMask[idx]; }
   void SetAll(bool to) { fMask.assign(fMask.size(), to); }
   void SetFirstEntry(Long64_t e) { fBegin = e; }
   void Union(const RMaskedEntryRange &other)
   {
      for (std::size_t i = 0u; i < fMask.size(); ++i)
         fMask[i] |= other[i];
   }
   std::size_t Count(std::size_t until) const { return std::accumulate(fMask.begin(), fMask.begin() + until, 0ul); }

   // Return std::numeric_limits<std::size_t>::max() if this mask is a superset of other.
   // Otherwise return the index of the first entry in other that is not contained in this mask.
   // Does _not_ check whether this->fBegin is equal to other.fBegin.
   std::size_t Contains(const RMaskedEntryRange &other, std::size_t until)
   {
      for (std::size_t i = 0u; i < until; ++i)
         if (other.fMask[i] && !fMask[i])
            return i;

      return std::numeric_limits<std::size_t>::max();
   }

   const ROOT::RVecB &GetMask() const { return fMask; }
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT

#endif
