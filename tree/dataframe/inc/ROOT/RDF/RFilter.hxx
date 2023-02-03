// Author: Enrico Guiraud, Danilo Piparo CERN  09/2018

/*************************************************************************
 * Copyright (C) 1995-2018, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RFILTER
#define ROOT_RFILTER

#include "ROOT/RDF/ColumnReaderUtils.hxx"
#include "ROOT/RDF/RColumnReaderBase.hxx"
#include "ROOT/RDF/RCutFlowReport.hxx"
#include "ROOT/RDF/Utils.hxx"
#include "ROOT/RDF/RFilterBase.hxx"
#include "ROOT/RDF/RLoopManager.hxx"
#include "ROOT/RDF/RMaskedEntryRange.hxx"
#include "ROOT/TypeTraits.hxx"
#include "RtypesCore.h"

#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility> // std::index_sequence
#include <vector>

namespace ROOT {

namespace Internal {
namespace RDF {
using namespace ROOT::Detail::RDF;

// fwd decl for RFilter
namespace GraphDrawing {
std::shared_ptr<GraphNode>
CreateFilterNode(const RFilterBase *filterPtr, std::unordered_map<void *, std::shared_ptr<GraphNode>> &visitedMap);

std::shared_ptr<GraphNode> AddDefinesToGraph(std::shared_ptr<GraphNode> node, const RColumnRegister &colRegister,
                                             const std::vector<std::string> &prevNodeDefines,
                                             std::unordered_map<void *, std::shared_ptr<GraphNode>> &visitedMap);
} // ns GraphDrawing

} // ns RDF
} // ns Internal

namespace Detail {
namespace RDF {
using namespace ROOT::TypeTraits;
namespace RDFGraphDrawing = ROOT::Internal::RDF::GraphDrawing;
class RJittedFilter;

template <typename FilterF, typename PrevNodeRaw>
class R__CLING_PTRCHECK(off) RFilter final : public RFilterBase {
   using ColumnTypes_t = typename CallableTraits<FilterF>::arg_types;
   using TypeInd_t = std::make_index_sequence<ColumnTypes_t::list_size>;
   // If the PrevNode is a RJittedFilter, treat it as a more generic RFilterBase: when dealing with systematic
   // variations we'll have a RJittedFilter node for the nominal case but other "universes" will use concrete filters,
   // so we normalize the "previous node type" to the base type RFilterBase.
   using PrevNode_t = std::conditional_t<std::is_same<PrevNodeRaw, RJittedFilter>::value, RFilterBase, PrevNodeRaw>;

   FilterF fFilter;
   /// Column readers per slot and per input column
   std::vector<std::array<RColumnReaderBase *, ColumnTypes_t::list_size>> fValueReaders;
   /// Arrays of type-erased raw pointers to the beginning of bulks of column values, one per slot.
   std::vector<std::array<void *, ColumnTypes_t::list_size>> fValuePtrs;
   const std::shared_ptr<PrevNode_t> fPrevNodePtr;
   PrevNode_t &fPrevNode;

public:
   RFilter(FilterF f, const ROOT::RDF::ColumnNames_t &columns, std::shared_ptr<PrevNode_t> pd,
           const RDFInternal::RColumnRegister &colRegister, std::string_view name = "",
           const std::string &variationName = "nominal")
      : RFilterBase(pd->GetLoopManagerUnchecked(), name, pd->GetLoopManagerUnchecked()->GetNSlots(), colRegister,
                    columns, pd->GetVariations(), variationName),
        fFilter(std::move(f)), fValueReaders(fLoopManager->GetNSlots()), fValuePtrs(fLoopManager->GetNSlots()),
        fPrevNodePtr(std::move(pd)), fPrevNode(*fPrevNodePtr)
   {
      fLoopManager->Register(this);
   }

   RFilter(const RFilter &) = delete;
   RFilter &operator=(const RFilter &) = delete;
   ~RFilter() {
      // must Deregister objects from the RLoopManager here, before the fPrevNode data member is destroyed:
      // otherwise if fPrevNode is the RLoopManager, it will be destroyed before the calls to Deregister happen.
      fLoopManager->Deregister(this);
   }

   const RDFInternal::RMaskedEntryRange &CheckFilters(unsigned int slot, Long64_t entry, std::size_t bulkSize) final
   {
      auto &mask = fMask[slot * RDFInternal::CacheLineStep<RDFInternal::RMaskedEntryRange>()];

      if (entry != mask.FirstEntry()) {
         mask = fPrevNode.CheckFilters(slot, entry, bulkSize);

         std::transform(fValueReaders[slot].begin(), fValueReaders[slot].end(), fValuePtrs[slot].begin(),
                        [&mask, &bulkSize](auto *v) { return v->Load(mask, bulkSize); });

         const std::size_t processed = mask.Count(bulkSize);
         std::size_t accepted = 0u;
         for (std::size_t i = 0ul; i < bulkSize; ++i) {
            auto &flag = mask[i];
            flag = flag && EvalFilter(slot, i, ColumnTypes_t{}, TypeInd_t{});
            accepted += flag;
         }

         fAccepted[slot * RDFInternal::CacheLineStep<ULong64_t>()] += accepted;
         fRejected[slot * RDFInternal::CacheLineStep<ULong64_t>()] += processed - accepted;
      }

      return mask;
   }

   template <typename... ColTypes, std::size_t... S>
   bool EvalFilter(unsigned int slot, std::size_t idx, TypeList<ColTypes...>, std::index_sequence<S...>)
   {
      return fFilter(*(static_cast<ColTypes *>(fValuePtrs[slot][S]) + idx)...);
      // avoid unused parameter warnings (gcc 12.1)
      (void)slot;
      (void)idx;
   }

   void InitSlot(TTreeReader *r, unsigned int slot) final
   {
      RDFInternal::RColumnReadersInfo info{fColumnNames, fColRegister, fIsDefine.data(), *fLoopManager};
      fValueReaders[slot] = RDFInternal::GetColumnReaders(slot, r, ColumnTypes_t{}, info, fVariation);
      fMask[slot].SetFirstEntry(-1ll);
   }

   // recursive chain of `Report`s
   void Report(ROOT::RDF::RCutFlowReport &rep) const final { PartialReport(rep); }

   void PartialReport(ROOT::RDF::RCutFlowReport &rep) const final
   {
      fPrevNode.PartialReport(rep);
      FillReport(rep);
   }

   void StopProcessing() final
   {
      ++fNStopsReceived;
      if (fNStopsReceived == fNChildren)
         fPrevNode.StopProcessing();
   }

   void IncrChildrenCount() final
   {
      ++fNChildren;
      // propagate "children activation" upstream. named filters do the propagation via `TriggerChildrenCount`.
      if (fNChildren == 1 && fName.empty())
         fPrevNode.IncrChildrenCount();
   }

   void TriggerChildrenCount() final
   {
      assert(!fName.empty()); // this method is to only be called on named filters
      fPrevNode.IncrChildrenCount();
   }

   void AddFilterName(std::vector<std::string> &filters) final
   {
      fPrevNode.AddFilterName(filters);
      auto name = (HasName() ? fName : "Unnamed Filter");
      filters.push_back(name);
   }

   /// Clean-up operations to be performed at the end of a task.
   void FinalizeSlot(unsigned int slot) final { fValueReaders[slot].fill(nullptr); }

   std::shared_ptr<RDFGraphDrawing::GraphNode>
   GetGraph(std::unordered_map<void *, std::shared_ptr<RDFGraphDrawing::GraphNode>> &visitedMap) final
   {
      // Recursively call for the previous node.
      auto prevNode = fPrevNode.GetGraph(visitedMap);
      const auto &prevColumns = prevNode->GetDefinedColumns();

      auto thisNode = RDFGraphDrawing::CreateFilterNode(this, visitedMap);

      /* If the returned node is not new, there is no need to perform any other operation.
       * This is a likely scenario when building the entire graph in which branches share
       * some nodes. */
      if (!thisNode->IsNew()) {
         return thisNode;
      }

      auto upmostNode = AddDefinesToGraph(thisNode, fColRegister, prevColumns, visitedMap);

      // Keep track of the columns defined up to this point.
      thisNode->AddDefinedColumns(fColRegister.GetNames());

      upmostNode->SetPrevNode(prevNode);
      return thisNode;
   }

   /// Return a clone of this Filter that works with values in the variationName "universe".
   std::shared_ptr<RNodeBase> GetVariedFilter(const std::string &variationName) final
   {
      // Only the nominal filter should be asked to produce varied filters
      assert(fVariation == "nominal");
      // nobody should ask for a varied filter for the nominal variation: they can just
      // use the nominal filter!
      assert(variationName != "nominal");
      // nobody should ask for a varied filter for a variation on which this filter does not depend:
      // they can just use the nominal filter.
      assert(RDFInternal::IsStrInVec(variationName, fVariations));

      auto it = fVariedFilters.find(variationName);
      if (it != fVariedFilters.end())
         return it->second;

      auto prevNode = fPrevNodePtr;
      if (static_cast<RNodeBase *>(fPrevNodePtr.get()) != static_cast<RNodeBase *>(fLoopManager) &&
          RDFInternal::IsStrInVec(variationName, prevNode->GetVariations()))
         prevNode = std::static_pointer_cast<PrevNode_t>(prevNode->GetVariedFilter(variationName));

      // the varied filters get a copy of the callable object.
      // TODO document this
      auto variedFilter = std::unique_ptr<RFilterBase>(
         new RFilter(fFilter, fColumnNames, std::move(prevNode), fColRegister, fName, variationName));
      auto e = fVariedFilters.insert({variationName, std::move(variedFilter)});
      return e.first->second;
   }
};

} // ns RDF
} // ns Detail
} // ns ROOT

#endif // ROOT_RFILTER
