// @(#)root/gl:$Name:  $:$Id: TGLPerspectiveCamera.cxx,v 1.6 2005/07/08 15:39:29 brun Exp $
// Author:  Richard Maunder  10/08/2005

/*************************************************************************
 * Copyright (C) 1995-2004, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include "TGLSAFrame.h"
#include "TGLSAViewer.h"

ClassImp(TGLSAFrame)

//______________________________________________________________________________
TGLSAFrame::TGLSAFrame(TGLSAViewer & viewer) :
   TGMainFrame(gClient->GetDefaultRoot()),
   fViewer(viewer)
{
}

//______________________________________________________________________________
TGLSAFrame::~TGLSAFrame()
{
}

//______________________________________________________________________________
Bool_t TGLSAFrame::ProcessMessage(Long_t msg, Long_t parm1, Long_t parm2)
{
   return fViewer.ProcessFrameMessage(msg, parm1, parm2);
}

//______________________________________________________________________________
void TGLSAFrame::CloseWindow()
{
   fViewer.Close();
}
