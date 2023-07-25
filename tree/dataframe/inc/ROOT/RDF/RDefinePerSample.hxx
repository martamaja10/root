// Author: Enrico Guiraud, CERN  08/2021

/*************************************************************************
 * Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RDEFINEPERSAMPLE
#define ROOT_RDF_RDEFINEPERSAMPLE

#include "ROOT/RDF/RLoopManager.hxx"
#include "ROOT/RDF/RSampleInfo.hxx"
#include "ROOT/RDF/Utils.hxx"
#include <ROOT/RDF/RDefineBase.hxx>
#include <ROOT/TypeTraits.hxx>

#include <vector>

namespace ROOT {
namespace Detail {
namespace RDF {

using namespace ROOT::TypeTraits;

template <typename F>
class R__CLING_PTRCHECK(off) RDefinePerSample final : public RDefineBase {
   using RetType_t = typename CallableTraits<F>::ret_type;

   F fExpression;
   std::vector<ROOT::RVec<RetType_t>> fLastResults;

public:
   RDefinePerSample(std::string_view name, std::string_view type, F expression, RLoopManager &lm)
      : RDefineBase(name, type, RDFInternal::RColumnRegister{nullptr}, lm, /*columnNames*/ {}),
        fExpression(std::move(expression)), fLastResults(lm.GetNSlots() * RDFInternal::CacheLineStep<RetType_t>())
   {
      for (auto &r : fLastResults)
         r.resize(fLoopManager->GetMaxEventsPerBulk());

      fLoopManager->Register(this);
      auto callUpdate = [this](unsigned int slot, const ROOT::RDF::RSampleInfo &id) { this->Update(slot, id); };
      fLoopManager->AddSampleCallback(this, std::move(callUpdate));
   }

   RDefinePerSample(const RDefinePerSample &) = delete;
   RDefinePerSample &operator=(const RDefinePerSample &) = delete;

   ~RDefinePerSample() { fLoopManager->Deregister(this); }

   /// Return the (type-erased) address of the Define'd value for the given processing slot.
   void *GetValuePtr(unsigned int slot) final
   {
      return static_cast<void *>(fLastResults[slot * RDFInternal::CacheLineStep<RetType_t>()].data());
   }

   void Update(unsigned int, const RDFInternal::RMaskedEntryRange &, std::size_t) final
   {
      // no-op
   }

   /// Update the values of the array starting at GetValuePtr().
   /// Even if all values will be equal we still fill an array of maxBulkSize values
   /// for consistency with the reading of other columns.
   void Update(unsigned int slot, const ROOT::RDF::RSampleInfo &id) final
   {
      const auto value = fExpression(slot, id);
      auto &results = fLastResults[slot * RDFInternal::CacheLineStep<RetType_t>()];
      for (std::size_t i = 0u; i < results.size(); ++i)
         results[i] = value;
   }

   const std::type_info &GetTypeId() const final { return typeid(RetType_t); }

   void InitSlot(TTreeReader *, unsigned int) final {}

   void FinalizeSlot(unsigned int) final {}

   // No-op for RDefinePerSample: it never depends on systematic variations
   void MakeVariations(const std::vector<std::string> &) final {}

   RDefineBase &GetVariedDefine(const std::string &) final
   {
      R__ASSERT(false && "This should never be called");
      return *this;
   }

   std::size_t GetTypeSize() const final { return sizeof(RetType_t); }

   bool IsDefinePerSample() const final { return true; }
};

} // namespace RDF
} // namespace Detail
} // namespace ROOT

#endif // ROOT_RDF_RDEFINEPERSAMPLE
