// Author: Enrico Guiraud 04/2023

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_REVENTMASK
#define ROOT_RDF_REVENTMASK

#include <ROOT/RDF/RMaskedEntryRange.hxx>
#include <ROOT/RVec.hxx>
#include <Rtypes.h>

namespace ROOT {
namespace RDF {
namespace Experimental {

/// A bitmask over a range of entries.
/// User-facing version of RMaskedEntryRange, acts as a view on the internal RMaskedEntryRange.
class REventMask {
   ROOT::RVec<bool> fMask; ///< The event mask. Its size is the number of events in the bulk.
   ULong64_t fBegin;       ///< Entry number of the first entry in the range this mask corresponds to.

public:
   REventMask(const ROOT::Internal::RDF::RMaskedEntryRange &m, std::size_t bulkSize)
      // this const_cast is harmless, the data in `m` cannot actually be modified via REventMask
      : fMask(const_cast<bool *>(&m[0]), bulkSize), fBegin(m.FirstEntry())
   {
   }
   Long64_t FirstEntry() const { return fBegin; }
   std::size_t Size() const { return fMask.size(); }
   /// There is no non-const equivalent. RMaskedEntryRange objects are expected to always be const-qualified.
   bool operator[](std::size_t idx) const { return fMask[idx]; }
};

} // namespace Experimental
} // namespace RDF
} // namespace ROOT

#endif
