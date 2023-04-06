// Author: Enrico Guiraud CERN 06/2023

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include <ROOT/RDF/RBulkReaderBase.hxx>

#include <stdexcept>

namespace ROOT {
namespace Internal {
namespace RDF {

RBulkReaderBase::RBulkReaderBase(TBranch &branch, TTree &tree)
   : fBuf(TBuffer::kWrite, 10000),
     fBranch(&branch),
     fFullBranchName(branch.GetFullName()),
     fChainOffset(branch.GetTree()->GetChainOffset()),
     fNotifyLink(this)
{
   if (auto *chain = dynamic_cast<TChain *>(&tree)) {
      // set up a callback that updates the branch of the reader when the chain changes sub-trees
      fNotifyLink.PrependLink(*chain);
      fChain = chain;
   }
}

RBulkReaderBase::~RBulkReaderBase()
{
   if (fChain)
      fNotifyLink.RemoveLink(*fChain);
}

bool RBulkReaderBase::Notify()
{
   // N.B. the current fBranch has been deleted at this point, do not use it here
   assert(fChain != nullptr && "We should only get notified if we have a fChain");

   fBranch = fChain->GetTree()->GetBranch(fFullBranchName.c_str());
   if (fBranch == nullptr) // try harder with FindBranch
      fBranch = fChain->FindBranch(fFullBranchName.c_str());
   if (fBranch == nullptr)
      throw std::runtime_error("RBulkReaderBase could not find back its branch in the chain after a switch in the "
                               "underlying TTree. This should never happen.");

   // reset internal state
   fBranchBulkBegin = 0ull;
   fBranchBulkSize = 0u;
   fLoadedEntriesBegin = -1ull;
   fChainOffset = fChain->GetChainOffset();

   return true;
}

} // namespace RDF
} // namespace Internal
} // namespace ROOT
