// \file ROOT/RFitPanelModel.hxx
/// \ingroup WebGui ROOT7
/// \author Sergey Linev <S.Linev@gsi.de>
/// \author Iliana Betsou <Iliana.Betsou@cern.ch>
/// \date 2019-04-11
/// \warning This is part of the ROOT 7 prototype! It will change without notice. It might trigger earthquakes. Feedback
/// is welcome!

/*************************************************************************
 * Copyright (C) 1995-2019, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RFitPanelModel
#define ROOT_RFitPanelModel

#include "Foption.h"
#include "Fit/DataRange.h"
#include "Math/MinimizerOptions.h"
#include "TString.h"

#include <vector>
#include <string>

class TH1;
class TF1;

namespace ROOT {
namespace Experimental {

/** Data structure for the fit panel */

struct RFitPanelModel {

   /// Generic item for ui5 ComboBox
   struct RComboBoxItem {
      std::string key;
      std::string value;
      RComboBoxItem() = default;
      RComboBoxItem(const std::string &_key, const std::string &_value) : key(_key), value(_value) {}
   };

   /// Entry in minimizer algorithm combo
   struct RMinimezerAlgorithm {
      int lib{0};        // to which library belongs
      int id{0};         // identifier
      std::string text;  // text shown in combobox
      RMinimezerAlgorithm() = default;
      RMinimezerAlgorithm(int _lib, int _id, const std::string &_text) : lib(_lib), id(_id), text(_text) {}
   };

   /// Basic function info, used in combo boxes
   struct RItemInfo {
      std::string group;
      std::string id;
      std::string name;

      RItemInfo() = default;
      RItemInfo(const std::string &_name) : group("Predefined"), name(_name) { id = "dflt::"; id.append(_name); }
      RItemInfo(const std::string &_group, const std::string &_id, const std::string &_name) : group(_group), id(_id), name(_name) {}
   };

   /// Function parameter info, used in edit parameters dialog

   struct RFuncPar {
      int ipar{0};
      std::string name;
      std::string value;
      bool fixed{false};
      std::string error;
      std::string min;
      std::string max;
      RFuncPar() = default;
      RFuncPar(int _ipar, const std::string &_name) : ipar(_ipar), name(_name) {}
   };

   /// Class used to transfer functions parameters list from/to client
   struct RFuncParsList {
      bool haspars{false};
      std::string id;                ///< function id in the FitPanel
      std::string name;              ///< display name
      std::vector<RFuncPar> pars;    ///< parameters
      void GetParameters(TF1 *f1);
      void SetParameters(TF1 *f1);
      void Clear();
   };

   std::string fTitle;                      ///< title of the fit panel

   std::vector<RItemInfo> fDataSet;     ///< list of available data sources
   std::string fSelectedData;               ///< selected data

   int fDim{0};                             ///< number of dimensions in selected data object

   std::vector<RItemInfo> fFuncList;     ///< all available fit functions
   std::string fSelectedFunc;               ///< id of selected fit function like dflt::gaus


   std::string fSelectedTab;               ///< key of selected tab, useful for drawing

   // General tab

   // Method
   std::vector<RComboBoxItem> fFitMethods;   ///< all supported fit methods
   std::string fFitMethod;                   ///< selected fit method

   bool fLinearFit{false};
   bool fRobust{false};
   float fRobustLevel{0.95};

   // Fit Options
   bool fIntegral{false};
   bool fUseRange{false};
   bool fBestErrors{false};
   bool fImproveFitResults{false};
   bool fAllWeights1{false};
   bool fAddToList{false};
   bool fEmptyBins1{false};
   bool fUseGradient{false};

   // Draw Options
   bool fSame{false};
   bool fNoDrawing{false};
   bool fNoStoreDraw{false};


   // all combo items for all methods

   // Minimization Tab
   int fLibrary{0};                                 ///< selected minimization library
   bool fHasGenetics{false};                        ///< is genetics available
   std::vector<RMinimezerAlgorithm> fMethodMinAll;  ///< all items for all methods
   int fSelectMethodMin{0};

   float fErrorDef{1.00};
   float fMaxTolerance{0.01};
   int fMaxIterations{0};
   int fPrint{0};


   // range selection, shown depending on fDim
   float fMinRangeX{0};
   float fMaxRangeX{1};
   float fStepX{0.01};
   float fRangeX[2] = {0,1};

   float fMinRangeY{0};
   float fMaxRangeY{1};
   float fStepY{0.01};
   float fRangeY[2] = {0,1};


   /// Parameters

   RFuncParsList fFuncPars;

   /////////Advanced Options

   bool fHasAdvanced{false};
   std::string fAdvancedTab;

   std::vector<RComboBoxItem> fContour1;
   std::string fContourPar1Id;
   std::vector<RComboBoxItem> fContour2;
   std::string fContourPar2Id;
   std::vector<RComboBoxItem> fScan;
   std::string fScanId;

   // Contour Tab
   int fContourPar1{0};
   int fContourPar2{0};
   int fContourPoints{0};
   float fConfLevel{0.};
   bool fContourImpose{false};
   std::string fColorContour[3];

   // Scan Tab
   int fScanPoints{0};
   int fScanPar{0};
   int fScanMin{0};
   int fScanMax{0};

   bool fInitialized{false};        ///<! indicates if data were initialized

   RFitPanelModel() = default;

   void Initialize();

   bool SelectHistogram(const std::string &hname, TH1 *hist);

   bool HasFunction(const std::string &id);

   void SelectedFunc(const std::string &name, TF1 *func);

   void UpdateRange(TH1 *hist);

   void UpdateAdvanced(TF1 *func);

   bool IsDataSelected() const { return !fSelectedData.empty(); }

   ROOT::Fit::DataRange GetRanges();
   Foption_t GetFitOptions();
   ROOT::Math::MinimizerOptions GetMinimizerOptions();

   TString GetDrawOption();
};

} // namespace Experimental
} // namespace ROOT

#endif
