// Author: Enrico Guiraud CERN 09/2020

/*************************************************************************
 * Copyright (C) 1995-2020, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_COLUMNREADERUTILS
#define ROOT_RDF_COLUMNREADERUTILS

#include "RBulkScalarReader.hxx"
#include "RBulkStaticArrayReader.hxx"
#include "RBulkDynamicArrayReader.hxx"
#include "RColumnReaderBase.hxx"
#include "RColumnRegister.hxx"
#include "RDefineBase.hxx"
#include "RDefineReader.hxx"
#include "RDSColumnReader.hxx"
#include "RLoopManager.hxx"
#include "RTreeColumnReader.hxx"
#include "RVariationBase.hxx"
#include "RVariationReader.hxx"
#include "ROOT/RVec.hxx"
#include "Utils.hxx" // SupportsBulkIO

#include <ROOT/RDataSource.hxx>
#include <ROOT/TypeTraits.hxx>
#include <TBranchElement.h> // for a dynamic_cast
#include <TTreeReader.h>

#include <array>
#include <cassert>
#include <map>
#include <memory>
#include <string>
#include <typeinfo> // for typeid
#include <vector>

namespace ROOT {
namespace Internal {
namespace RDF {

using namespace ROOT::TypeTraits;
namespace RDFDetail = ROOT::Detail::RDF;

// Return a pointer to the requested branch if we can do bulk I/O with it, nullptr otherwise.
inline TBranch *GetBranchForBulkIO(TTreeReader &r, const std::string &colName)
{
   TBranch *b = r.GetTree()->GetBranch(colName.c_str());
   if (!b) // try harder, with FindBranch
      b = r.GetTree()->FindBranch(colName.c_str());
   if (!b) {
      const auto msg = "Could not find branch corresponding to column " + colName + ". This should never happen.";
      throw std::runtime_error(std::move(msg));
   }

   // Some TBranchElements support bulk I/O but things can get really hairy when a TBranchElement contains a
   // variable-sized array and its size is contained in the parent TBranchElement. Much simpler to use TTreeReader.
   if (!b->SupportsBulkRead() || dynamic_cast<TBranchElement *>(b))
      return nullptr;

   TLeaf *l = static_cast<TLeaf *>(b->GetListOfLeaves()->UncheckedAt(0));
   TLeaf *lc = l->GetLeafCount();
   if (lc && l->GetLenStatic() > 1)
      return nullptr; // bulk I/O supports 2D arrays (1 static and one dynamic dimension) but RDF does not

   // otherwise we are ok for bulk I/O of this branch so we return a non-null branch address
   return b;
}

template <typename T>
RDFDetail::RColumnReaderBase *
MakeTreeReaderColumnReader(TTreeReader &r, const std::string &colName, unsigned int slot, RLoopManager &lm)
{
   auto treeColReader = std::make_unique<RTreeColumnReader<T>>(r, colName, lm.GetMaxEventsPerBulk());
   return lm.AddTreeColumnReader(slot, colName, std::move(treeColReader), typeid(T));
}

/// Create a RBulkDynamicArrayReader, also creating a reader for its size if needed.
template <typename ColType, typename SizeType>
RDFDetail::RColumnReaderBase *
MakeBulkDynArrReader(unsigned int slot, RLoopManager &lm, TTree &t, TBranch &sizeBranch, TBranch &valueBranch)
{
   const auto maxBulkSize = lm.GetMaxEventsPerBulk();

   // get (or create) the reader for the array size
   auto sizeColName = std::string(sizeBranch.GetFullName());
   RColumnReaderBase *sizeReaderPtr = lm.GetDatasetColumnReader(slot, sizeColName, typeid(SizeType));
   if (!sizeReaderPtr) {
      std::unique_ptr<RColumnReaderBase> sizeReader(new RBulkScalarReader<SizeType>(sizeBranch, t, maxBulkSize));
      sizeReaderPtr = lm.AddTreeColumnReader(slot, sizeColName, std::move(sizeReader), typeid(SizeType));
   }

   return new RBulkDynamicArrayReader<ColType, SizeType>(valueBranch, t, *sizeReaderPtr, maxBulkSize);
}

/// Build the appropriate reader to do bulk I/O on a TBranch.
/// We want to build a RBulkScalarReader, RBulkStaticArrayReader or RBulkDynamicArrayReader depending on the kind of
/// branch we are reading.
template <typename T>
RDFDetail::RColumnReaderBase *MakeBulkColumnReader(unsigned int slot, RLoopManager &lm, TTreeReader &r, TBranch &branch,
                                                   const std::string &colName, T *)
{
   std::unique_ptr<RColumnReaderBase> bulkReader;

   if constexpr (!VecOps::IsRVec<T>()) { // reading a scalar
      bulkReader.reset(new RBulkScalarReader<T>(branch, *r.GetTree(), lm.GetMaxEventsPerBulk()));
   } else {
      // reading an array with static or dynamic size
      // this is a simple TBranch that supports bulk reading: it must have one leaf
      TLeaf *l = static_cast<TLeaf *>(branch.GetListOfLeaves()->UncheckedAt(0));
      TLeaf *lc = l->GetLeafCount();

      if (!lc) { // array with static size
         const auto staticSize = l->GetLenStatic();
         bulkReader.reset(new RBulkStaticArrayReader<T>(branch, *r.GetTree(), staticSize, lm.GetMaxEventsPerBulk()));
      } else { // array with dynamic size
         TBranch *sizeBranch = lc->GetBranch();
         assert(sizeBranch != nullptr);
         const std::string leafType = lc->GetTypeName();
         TTree &t = *r.GetTree();
         RColumnReaderBase *bulkReaderPtr = nullptr;
         if (leafType == "Int_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, Int_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "UInt_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, UInt_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "Short_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, Short_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "UShort_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, UShort_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "Long_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, Long_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "ULong_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, ULong_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "Long64_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, Long64_t>(slot, lm, t, *sizeBranch, branch);
         } else if (leafType == "ULong64_t") {
            bulkReaderPtr = MakeBulkDynArrReader<T, ULong64_t>(slot, lm, t, *sizeBranch, branch);
         }
         bulkReader.reset(bulkReaderPtr);
      }
   }

   return lm.AddTreeColumnReader(slot, colName, std::move(bulkReader), typeid(T));
}

// never returns a nullptr
template <typename T>
RDFDetail::RColumnReaderBase *GetColumnReader(unsigned int slot, RColumnReaderBase *defineOrVariationReader,
                                              RLoopManager &lm, TTreeReader *r, const std::string &colName)
{
   if (defineOrVariationReader != nullptr)
      return defineOrVariationReader;

   // Check if we already inserted a reader for this column in the dataset column readers (RDataSource or Tree/TChain
   // readers)
   auto *datasetColReader = lm.GetDatasetColumnReader(slot, colName, typeid(T));
   if (datasetColReader != nullptr)
      return datasetColReader;

   assert(r != nullptr && "We could not find a reader for this column, this should never happen at this point.");

   if constexpr (SupportsBulkIO<T>) {
      TBranch *b = GetBranchForBulkIO(*r, colName);
      if (b)
         return MakeBulkColumnReader(slot, lm, *r, *b, colName, (T *)nullptr);
   }

   // otherwise use a normal (slower) RTreeColumnReader backed by a TTreeReaderValue or a TTreeReaderArray.
   return MakeTreeReaderColumnReader<T>(*r, colName, slot, lm);
}

/// This type aggregates some of the arguments passed to GetColumnReaders.
/// We need to pass a single RColumnReadersInfo object rather than each argument separately because with too many
/// arguments passed, gcc 7.5.0 and cling disagree on the ABI, which leads to the last function argument being read
/// incorrectly from a compiled GetColumnReaders symbols when invoked from a jitted symbol.
struct RColumnReadersInfo {
   const std::vector<std::string> &fColNames;
   RColumnRegister &fColRegister;
   const bool *fIsDefine;
   RLoopManager &fLoopManager;
};

/// Create a group of column readers, one per type in the parameter pack.
template <typename... ColTypes>
std::array<RDFDetail::RColumnReaderBase *, sizeof...(ColTypes)>
GetColumnReaders(unsigned int slot, TTreeReader *r, TypeList<ColTypes...>, const RColumnReadersInfo &colInfo,
                 const std::string &variationName = "nominal")
{
   // see RColumnReadersInfo for why we pass these arguments like this rather than directly as function arguments
   const auto &colNames = colInfo.fColNames;
   auto &lm = colInfo.fLoopManager;
   auto &colRegister = colInfo.fColRegister;

   int i = -1;
   std::array<RDFDetail::RColumnReaderBase *, sizeof...(ColTypes)> ret{
      (++i, GetColumnReader<ColTypes>(slot, colRegister.GetReader(slot, colNames[i], variationName, typeid(ColTypes)),
                                      lm, r, colNames[i]))...};
   return ret;
}

// Shortcut overload for the case of no columns
inline std::array<RDFDetail::RColumnReaderBase *, 0>
GetColumnReaders(unsigned int, TTreeReader *, TypeList<>, const RColumnReadersInfo &, const std::string & = "nominal")
{
   return {};
}

} // namespace RDF
} // namespace Internal
} // namespace ROOT

#endif // ROOT_RDF_COLUMNREADERS
