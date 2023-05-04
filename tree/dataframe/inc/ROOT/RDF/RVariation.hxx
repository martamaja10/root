// Author: Enrico Guiraud, CERN 10/2021

/*************************************************************************
 * Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RVARIATION
#define ROOT_RDF_RVARIATION

#include "ColumnReaderUtils.hxx"
#include "RColumnReaderBase.hxx"
#include "RLoopManager.hxx"
#include "RVariationBase.hxx"

#include <string_view>
#include <ROOT/TypeTraits.hxx>
#include <Rtypes.h> // R__CLING_PTRCHECK, Long64_t

#include <array>
#include <deque>
#include <map>
#include <string>
#include <type_traits> // std::is_same, std::conditional_t
#include <utility>     // std::index_sequence
#include <vector>

class TTreeReader;

namespace ROOT {

namespace RDF {
class RDataSource;
}

namespace Internal {
namespace RDF {

using namespace ROOT::TypeTraits;

/// @name Helper functions for the case of a single column being varied.
///@{
template <typename T>
bool ResultsSizeEq(const T &results, std::size_t expected, std::size_t nColumns, std::true_type /*isSingleColumn*/)
{
   assert(nColumns == 1);
   (void)nColumns;

   return results.size() == expected;
}

template <typename T>
std::size_t GetNVariations(const RVec<RVec<T>> &results)
{
   return results.size();
}

template <typename T>
void ResizeResults(ROOT::RVec<T> &results, std::size_t /*nCols*/, std::size_t nVariations, std::size_t maxBulkSize)
{
   results.resize(nVariations);
   for (auto &bulkVec : results)
      bulkVec.resize(maxBulkSize);
}

/// Assign into fLastResults[slot] without changing the addresses of its elements (we gave those addresses away in
/// GetValuePtr)
/// The callee is responsible of making sure that `resStorage` has the correct size.
template <typename T>
void AssignResults(ROOT::RVec<ROOT::RVec<T>> &resStorage, ROOT::RVec<T> &&tmpResults, std::size_t bulkIdx)
{
   const auto nVariations = resStorage.size(); // we have already checked that tmpResults has the same size

   for (auto i = 0u; i < nVariations; ++i)
      resStorage[i][bulkIdx] = std::move(tmpResults[i]);
}

template <typename T>
void *GetValuePtrHelper(ROOT::RVec<ROOT::RVec<T>> &v, std::size_t /*colIdx*/, std::size_t varIdx)
{
   return static_cast<void *>(&v[varIdx][0]);
}
///@}

/// @name Helper functions for the case of multiple columns being varied simultaneously.
///@{
template <typename T>
bool ResultsSizeEq(const T &results, std::size_t expected, std::size_t /*nColumns*/, std::false_type /*isSingleColumn*/)
{
   return std::all_of(results.begin(), results.end(),
                      [expected](const auto &inner) { return inner.size() == expected; });
}

template <typename T>
std::size_t GetNVariations(const std::vector<RVec<ROOT::RVec<T>>> &results)
{
   assert(!results.empty());
   return results[0].size();
}

template <typename T>
void ResizeResults(std::vector<ROOT::RVec<T>> &results, std::size_t nCols, std::size_t nVariations,
                   std::size_t maxBulkSize)
{
   results.resize(nCols);
   for (auto &rvecOverVariations : results) {
      rvecOverVariations.resize(nVariations);
      for (auto &rvecOverBulk : rvecOverVariations)
         rvecOverBulk.resize(maxBulkSize);
   }
}

// The callee is responsible of making sure that `resStorage` has the correct outer and inner sizes.
template <typename T>
void AssignResults(std::vector<ROOT::RVec<ROOT::RVec<T>>> &resStorage, ROOT::RVec<ROOT::RVec<T>> &&tmpResults,
                   std::size_t bulkIdx)
{
   const auto nCols = resStorage.size();
   const auto nVariations = resStorage[0].size();
   for (auto colIdx = 0u; colIdx < nCols; ++colIdx)
      for (auto varIdx = 0u; varIdx < nVariations; ++varIdx)
         resStorage[colIdx][varIdx][bulkIdx] = std::move(tmpResults[colIdx][varIdx]);
}

template <typename T>
void *GetValuePtrHelper(std::vector<ROOT::RVec<ROOT::RVec<T>>> &v, std::size_t colIdx, std::size_t varIdx)
{
   return static_cast<void *>(&v[colIdx][varIdx][0]);
}
///@}

template <typename VaryExpressionRet_t, bool IsSingleColumn>
struct ColumnType {
};

template <typename T>
struct ColumnType<ROOT::RVec<T>, true> {
   using type = T;
};

template <typename T>
struct ColumnType<ROOT::RVec<ROOT::RVec<T>>, false> {
   using type = T;
};

/// When varying a single column, Ret_t is RVec<T> and ColumnType_t is T.
/// When varying multiple columns, Ret_t is RVec<RVec<T>> and ColumnType_t is T.
template <bool IsSingleColumn, typename Ret_t>
using ColumnType_t = typename ColumnType<Ret_t, IsSingleColumn>::type;

template <typename F, bool IsSingleColumn>
class R__CLING_PTRCHECK(off) RVariation final : public RVariationBase {
   using ColumnTypes_t = typename CallableTraits<F>::arg_types;
   using TypeInd_t = std::make_index_sequence<ColumnTypes_t::list_size>;
   using Ret_t = typename CallableTraits<F>::ret_type;
   using VariedCol_t = ColumnType_t<IsSingleColumn, Ret_t>;
   using Result_t = std::conditional_t<IsSingleColumn, ROOT::RVec<ROOT::RVec<VariedCol_t>>,
                                       std::vector<ROOT::RVec<ROOT::RVec<VariedCol_t>>>>;

   F fExpression;
   /// Per-slot storage for varied column values (for one or multiple columns depending on IsSingleColumn).
   /// Dimensions from inner to outer: bulk idx, variation idx[, column idx], slot idx.
   std::vector<Result_t> fLastResults;

   /// Column readers per slot and per input column
   std::vector<std::array<RColumnReaderBase *, ColumnTypes_t::list_size>> fValueReaders;

   /// Arrays of type-erased raw pointers to the beginning of bulks of column values, one per slot.
   std::vector<std::array<void *, ColumnTypes_t::list_size>> fValuePtrs;

   template <typename... ColTypes, std::size_t... S>
   void UpdateHelper(unsigned int slot, std::size_t idx, TypeList<ColTypes...>, std::index_sequence<S...>)
   {
      // fExpression must return an RVec<T>
      auto &&results = fExpression(*(static_cast<ColTypes *>(fValuePtrs[slot][S]) + idx)...);

      /* FIXME this is expensive, can we do something different?
      if (!ResultsSizeEq(results, fVariationNames.size(), fColNames.size(),
                         std::integral_constant<bool, IsSingleColumn>{})) {
         std::string variationName = fVariationNames[0].substr(0, fVariationNames[0].find_first_of(':'));
         throw std::runtime_error("The evaluation of the expression for variation \"" + variationName +
                                  "\" resulted in " + std::to_string(GetNVariations(results)) + " values, but " +
                                  std::to_string(fVariationNames.size()) + " were expected.");
      }
      */

      AssignResults(fLastResults[slot * CacheLineStep<Result_t>()], std::move(results), idx);
      (void)idx; // avoid unused parameter warnings (gcc 12.2)
   }

public:
   RVariation(const std::vector<std::string> &colNames, std::string_view variationName, F expression,
              const std::vector<std::string> &variationTags, std::string_view type, const RColumnRegister &defines,
              RLoopManager &lm, const ColumnNames_t &inputColNames)
      : RVariationBase(colNames, variationName, variationTags, type, defines, lm, inputColNames),
        fExpression(std::move(expression)),
        fLastResults(lm.GetNSlots() * CacheLineStep<Result_t>()),
        fValueReaders(lm.GetNSlots()),
        fValuePtrs(lm.GetNSlots())
   {
      fLoopManager->Register(this);

      for (auto i = 0u; i < lm.GetNSlots(); ++i)
         ResizeResults(fLastResults[i * CacheLineStep<Result_t>()], colNames.size(), variationTags.size(),
                       fLoopManager->GetMaxEventsPerBulk());
   }

   RVariation(const RVariation &) = delete;
   RVariation &operator=(const RVariation &) = delete;
   ~RVariation() { fLoopManager->Deregister(this); }

   void InitSlot(TTreeReader *r, unsigned int slot) final
   {
      RColumnReadersInfo info{fInputColumns, fColumnRegister, fIsDefine.data(), *fLoopManager};
      fValueReaders[slot] = GetColumnReaders(slot, r, ColumnTypes_t{}, info);
      fMask[slot].SetFirstEntry(-1ll);
   }

   /// Return the (type-erased) address of the value for the given processing slot.
   void *GetValuePtr(unsigned int slot, const std::string &column, const std::string &variation) final
   {
      const auto colIt = std::find(fColNames.begin(), fColNames.end(), column);
      assert(colIt != fColNames.end());
      const auto colIdx = std::distance(fColNames.begin(), colIt);

      const auto varIt = std::find(fVariationNames.begin(), fVariationNames.end(), variation);
      assert(varIt != fVariationNames.end());
      const auto varIdx = std::distance(fVariationNames.begin(), varIt);

      return GetValuePtrHelper(fLastResults[slot * CacheLineStep<Result_t>()], colIdx, varIdx);
   }

   /// Update the value at the address returned by GetValuePtr with the content corresponding to the given entry
   void Update(unsigned int slot, const RMaskedEntryRange &requestedMask, std::size_t bulkSize) final
   {
      auto &valueMask = fMask[slot * RDFInternal::CacheLineStep<RDFInternal::RMaskedEntryRange>()];
      // Index of the first entry in the bulk for which we do not already have a value
      std::size_t firstNewIdx = std::numeric_limits<std::size_t>::max();
      if (valueMask.FirstEntry() != requestedMask.FirstEntry()) { // new bulk
         // if it turns out that we do these two operations together very often, maybe it's worth having a ad-hoc method
         valueMask.SetAll(false);
         valueMask.SetFirstEntry(requestedMask.FirstEntry());
         firstNewIdx = 0u;
      } else if ((firstNewIdx = valueMask.Contains(requestedMask, bulkSize)) == std::numeric_limits<std::size_t>::max()) {
         // this is a common occurrence: it happens when the same Vary result is used multiple times downstream of the
         // same Filters -- nothing to do.
         return;
      }

      std::transform(fValueReaders[slot].begin(), fValueReaders[slot].end(), fValuePtrs[slot].begin(),
                     [&requestedMask, &bulkSize](auto *v) { return v->Load(requestedMask, bulkSize); });

      for (std::size_t i = firstNewIdx; i < bulkSize; ++i) {
         if (requestedMask[i] && !valueMask[i]) { // we don't have a value for this entry yet
            UpdateHelper(slot, i, ColumnTypes_t{}, TypeInd_t{});
            valueMask[i] = true;
         }
      }
   }

   const std::type_info &GetTypeId() const final { return typeid(VariedCol_t); }

   /// Clean-up operations to be performed at the end of a task.
   void FinalizeSlot(unsigned int slot) final { fValueReaders[slot].fill(nullptr); }
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT

#endif // ROOT_RDF_RVARIATION
