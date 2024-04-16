#  @author Vincenzo Eduardo Padulano
#  @author Enric Tejedor
#  @date 2021-02

################################################################################
# Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.                      #
# All rights reserved.                                                         #
#                                                                              #
# For the licensing terms see $ROOTSYS/LICENSE.                                #
# For the list of contributors see $ROOTSYS/README/CREDITS.                    #
################################################################################
from __future__ import annotations

import logging
import types

import concurrent.futures

from typing import Iterable, TYPE_CHECKING

from DistRDF.Backends import build_backends_submodules
from DistRDF.LiveVisualize import LiveVisualize

if TYPE_CHECKING:
    from DistRDF.Proxy import ResultPtrProxy, ResultMapProxy

logger = logging.getLogger(__name__)


def initialize(fun, *args, **kwargs):
    """
    Set a function that will be executed as a first step on every backend before
    any other operation. This method also executes the function on the current
    user environment so changes are visible on the running session.

    This allows users to inject and execute custom code on the worker
    environment without being part of the RDataFrame computational graph.

    Args:
        fun (function): Function to be executed.

        *args (list): Variable length argument list used to execute the
            function.

        **kwargs (dict): Keyword arguments used to execute the function.
    """
    from DistRDF.Backends import Base
    Base.BaseBackend.register_initialization(fun, *args, **kwargs)

    #Base.BaseBackend.register_initialization(funcs, *args, **kwargs, RunOncePerProc = True, df = None)


def RunGraphs(proxies: Iterable) -> int:
    """
    Trigger the execution of multiple RDataFrame computation graphs on a certain
    distributed backend. If the backend doesn't support multiple job
    submissions concurrently, the distributed computation graphs will be
    executed sequentially.

    Args:
        proxies(list): List of action proxies that should be triggered. Only
            actions belonging to different RDataFrame graphs will be
            triggered to avoid useless calls.

    Return:
        (int): The number of unique computation graphs executed by this call.


    Example:

        @code{.py}
        import ROOT
        RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame
        RunGraphs = ROOT.RDF.Experimental.Distributed.RunGraphs

        # Create 3 different dataframes and book an histogram on each one
        histoproxies = [
            RDataFrame(100)
                .Define("x", "rdfentry_")
                .Histo1D(("name", "title", 10, 0, 100), "x")
            for _ in range(4)
        ]

        # Execute the 3 computation graphs
        n_graphs_run = RunGraphs(histoproxies)
        # Retrieve all the histograms in one go
        histos = [histoproxy.GetValue() for histoproxy in histoproxies]
        @endcode


    """
    # Import here to avoid circular dependencies in main module
    from DistRDF.Proxy import execute_graph
    if not proxies:
        logger.warning("RunGraphs: Got an empty list of handles, now quitting.")
        return 0

    # Get proxies belonging to distinct computation graphs
    uniqueproxies = list({proxy.proxied_node.get_head(): proxy for proxy in proxies}.values())

    # Submit all computation graphs concurrently from multiple Python threads.
    # The submission is not computationally intensive
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(uniqueproxies)) as executor:
        futures = [executor.submit(execute_graph, proxy.proxied_node) for proxy in uniqueproxies]
        concurrent.futures.wait(futures)

    return len(uniqueproxies)


def VariationsFor(actionproxy: ResultPtrProxy) -> ResultMapProxy:
    """
    Equivalent of ROOT.RDF.Experimental.VariationsFor in distributed mode.
    """
    # similar to resPtr.fActionPtr->MakeVariedAction()
    return actionproxy.create_variations()


def create_distributed_module(parentmodule):
    """
    Helper function to create the ROOT.RDF.Experimental.Distributed module.

    Users will see this module as the entry point of functions to create and
    run an RDataFrame computation distributedly.
    """
    distributed = types.ModuleType("ROOT.RDF.Experimental.Distributed")

    # PEP302 attributes
    distributed.__file__ = "<module ROOT.RDF.Experimental>"
    # distributed.__name__ is the constructor argument
    distributed.__path__ = []  # this makes it a package
    # distributed.__loader__ is not defined
    distributed.__package__ = parentmodule

    distributed = build_backends_submodules(distributed)

    # Inject top-level functions
    distributed.initialize = initialize
    distributed.RunGraphs = RunGraphs
    distributed.VariationsFor = VariationsFor
    distributed.LiveVisualize = LiveVisualize

    distributed.DeclareCppCode = DeclareCppCode
    distributed.UploadFilesAndLoadSharedLib = UploadFilesAndLoadSharedLib
    distributed.LoadSharedLib = LoadSharedLib
    distributed.DistributeFiles = DistributeFiles
    distributed.CompileMacro = CompileMacro
    
    return distributed


#def DeclareCppCode(code_to_declare, RunOncePerProcess = True, df = None) -> None:
def DeclareCppCode(code_to_declare) -> None:

    """
    Declare the C++ code that has to be processed on each worker. 
    Args:
        codeToDeclare (_type_): _description_
        df (_type_, optional): _description_. Defaults to None.
    """
    
    from DistRDF.Backends import Base
    #Base.BaseBackend.register_declaration(code_to_declare, df)
    Base.BaseBackend.register_declaration(code_to_declare)


# simplifies 
# ROOT.gInterpreter.AddIncludePath -- done by distribute_headers
# ROOT.gSystem.AddDynamicPath  -- done by distribute_shared_libraries
# ROOT.gSystem.Load -- done by distribute_shared_libraries
# AS: 
# distribute_shared_libraries checks for paths to libraries and pcm files 
# and then calls  Utils.declare_shared_libraries which calls ROOT.gSystem.Load
def UploadFilesAndLoadSharedLib(paths_to_shared_libraries, necessary_headers, df) -> None:
    from DistRDF.Backends import Utils
    
    df._headnode.backend.distribute_unique_paths(necessary_headers)

    libraries_to_distribute = set()
    pcm_to_distribute = set()
    if isinstance(paths_to_shared_libraries, str):
        pcm_to_distribute, libraries_to_distribute = (
            Utils.check_pcm_in_library_path(paths_to_shared_libraries))
    else:
        for path_string in paths_to_shared_libraries:
            pcm, libraries = Utils.check_pcm_in_library_path(
                path_string
            )
            libraries_to_distribute.update(libraries)
            pcm_to_distribute.update(pcm)
    # Distribute shared libraries and pcm files to the workers
    df._headnode.backend.distribute_unique_paths(libraries_to_distribute)
    df._headnode.backend.distribute_unique_paths(pcm_to_distribute)
    # Include shared libraries locally
    Utils.declare_shared_libraries(libraries_to_distribute) #libraries loading happens here
    
    
    
    # this won't work because distribute_headers is non-static
    # from DistRDF.Backends import Base
    # Base.BaseBackend.distribute_headers(necessary_headers) # or a path to shared fs where all the headers are stored
    # Base.BaseBackend.distribute_shared_libraries(paths_to_shared_libraries)

def LoadSharedLib(paths_to_shared_libraries) -> None:
    from DistRDF.Backends import Base
    # b = Base.BaseBackend()
    # b.distribute_shared_libraries(paths_to_shared_libraries)
    Base.BaseBackend.distribute_shared_libraries(paths_to_shared_libraries)
        
# simplifies: d._headnode.backend.distribute_unique_paths([str]) in the user's code
# for now doesn't work even though now distribute_files is a static method so no need to initialize the class 
# I can't initialize the class because there are abstract methods
# 
def DistributeFiles(paths_to_files, df):
    df._headnode.backend.distribute_unique_paths(paths_to_files)  #this works
    
    # else:
    #     from DistRDF.Backends import Base
    #     Base.BaseBackend.distribute_files(paths_to_files)    
        
        
    #from DistRDF.Backends import Base
    #b = Base.BaseBackend()
    #b.distribute_files(paths_to_files)
    #from DistRDF.Backends.Dask import Backend
    # b = Backend.DaskBackend()
    
    #from DistRDF.HeadNode import HeadNode
    # b = 
        
        
        # backend = HeadNode.backend()
        # df.backend.distribute_files(paths_to_files)
            
            #     headnode = HeadNode.get_headnode(backend, npartitions, treename, filenames)
            # rdf = DataFrame.RDataFrame(headnode)        
            
    # b.distribute_files(paths_to_files)
    # Base.BaseBackend.distribute_files(paths_to_files)
   
# not sure if this is needed not in the context of LoadSharedLib and DistributeFiles?     
# simplifies ROOT.gInterpreter.AddIncludePath - do we need it? 
# def IncludeHeader(path_to_headers):
#     from DistRDF.Backends import Base
#     Base.BaseBackend.register_declaration(, df = None)
    
def CompileMacro(paths_to_macros_to_compile) -> None: 
    from DistRDF.Backends import Base
    Base.BaseBackend.compile_macro(paths_to_macros_to_compile)
    
