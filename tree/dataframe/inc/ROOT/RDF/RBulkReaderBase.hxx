// Author: Enrico Guiraud CERN 04/2023

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RBULKREADERBASE
#define ROOT_RDF_RBULKREADERBASE

#include <ROOT/RDF/RColumnReaderBase.hxx>
#include <ROOT/RDF/RMaskedEntryRange.hxx>

#include <Rtypes.h>
#include <TChain.h>
#include <TBranch.h>
#include <TBufferFile.h>
#include <TNotifyLink.h>

#include <iostream>

namespace ROOT {
namespace Internal {
namespace RDF {

class RBulkReaderBase : public ROOT::Detail::RDF::RColumnReaderBase {
protected:
   TBufferFile fBuf; ///< Buffer into which we will load branch values.
   TBranch *fBranch; ///< Branch to load values from. Never null.
   /// The full branch name: fBranch might be deleted as we switch to a new sub-tree in a chain and we use the full name
   /// to get the same branch in the new sub-tree.
   std::string fFullBranchName;
   TChain *fChain = nullptr;             ///< The chain we are reading values from. Null if reading from TTree.
   ULong64_t fChainOffset;               ///< Offset of the first entry of the current tree in the chain.
   ULong64_t fBranchBulkBegin = 0ull;    ///< Global entry number of first entry loaded in fBuf.
   std::size_t fBranchBulkSize = 0u;     ///< Number of entries loaded in fBuf.
   Long64_t fLoadedEntriesBegin = -1ull; ///< Global entry number of the last RDF bulk we loaded.
   /// A notifier class that we register in the TChain so our Notify() method
   /// gets called if the underlying tree changes (so we can update fBranch).
   TNotifyLink<RBulkReaderBase> fNotifyLink;

public:
   RBulkReaderBase(TBranch &branch, TTree &tree);
   virtual ~RBulkReaderBase();

   /// Update RBulkReader state when a TChain switches to a new TTree.
   /// Called by fNotifyLink, called by TChain::LoadTree.
   /// \warn This Notify() is only called when the TChain switches to
   ///       to a new TTree _after the first_ (the RBulkReader does not
   ///       exist yet when the TChain loads the very first tree).
   ///       Do not rely on this method to set the initial state of
   ///       RBulkReader, use the constructor for that.
   bool Notify();
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT
#endif
