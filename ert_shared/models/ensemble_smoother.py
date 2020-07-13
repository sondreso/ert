from res.enkf.enums import HookRuntime
from res.enkf.enums import RealizationStateEnum
from res.enkf import ErtRunContext, EnkfSimulationRunner
from ert_shared.models import BaseRunModel, ErtRunError
from ert_shared import ERT

from ert_shared.storage.extraction_api import dump_to_new_storage
class EnsembleSmoother(BaseRunModel):

    def __init__(self):
        super(EnsembleSmoother, self).__init__(ERT.enkf_facade.get_queue_config() , phase_count=2)
        self.support_restart = False

    def setAnalysisModule(self, module_name):
        module_load_success = self.ert().analysisConfig().selectModule(module_name)

        if not module_load_success:
            raise ErtRunError("Unable to load analysis module '%s'!" % module_name)


    def runSimulations(self, arguments):
        prior_context = self.create_context( arguments )

        self.checkMinimumActiveRealizations(prior_context)
        self.setPhase(0, "Running simulations...", indeterminate=False)

        # self.setAnalysisModule(arguments["analysis_module"])

        self.setPhaseName("Pre processing...", indeterminate=True)

        # This line will initialize parameters in storage
        self.ert().getEnkfSimulationRunner().createRunPath(prior_context)

        ####################################################################################
        from res.enkf import EnkfConfigNode, EnkfNode, NodeId
        facade = ERT.enkf_facade

        test_fs = ERT.ert.getEnkfFsManager().getFileSystem("hurrdurr")
        test_sim_fs = prior_context.get_sim_fs()

        self.ert().getEnkfFsManager().switchFileSystem(test_fs)

        enkf_config_node = self.ert().ensembleConfig().getNode("COEFFS")
        assert isinstance(enkf_config_node, EnkfConfigNode)

        ERT.ert.getEnkfFsManager().initializeCaseFromExisting(prior_context.get_sim_fs(),0, test_fs)
        print(test_fs.getCaseName())
        for i in range(5):
            node = EnkfNode(enkf_config_node)
            gkw = node.asGenKw()
            gkw["COEFF_A"] = 82.0*i
            gkw["COEFF_B"] = 161.0*i
            gkw["COEFF_C"] = 21.0*i
            node.save(test_fs, NodeId( 0 , i ))
            print("\t", gkw.items())

        test_fs.fsync()

        node_old = EnkfNode(enkf_config_node)
        print(test_sim_fs.getCaseName())
        for i in range(5):
            node_old.load(test_sim_fs, NodeId( 0 , i ))
            gkw_old = node_old.asGenKw()
            print("\t", gkw_old.items())

        print(test_fs.getStateMap())
        print(test_sim_fs.getStateMap())


        print(facade.gather_gen_kw_data("hurrdurr", "COEFFS:COEFF_A"))
        print(facade.gather_gen_kw_data("default", "COEFFS:COEFF_A"))
        print(facade.gather_gen_kw_data("tu", "COEFFS:COEFF_A"))
        self.ert().getEnkfFsManager().switchFileSystem(test_sim_fs)
        print("Done")
        ####################################################################################

        EnkfSimulationRunner.runWorkflows(HookRuntime.PRE_SIMULATION, ert=ERT.ert)

        self.setPhaseName("Running forecast...", indeterminate=False)
        self._job_queue = self._queue_config.create_job_queue( )

        # This line will store results in storage (presumably in callback functions)
        num_successful_realizations = self.ert().getEnkfSimulationRunner().runSimpleStep(self._job_queue, prior_context)

        self.checkHaveSufficientRealizations(num_successful_realizations)

        self.setPhaseName("Post processing...", indeterminate=True)
        EnkfSimulationRunner.runWorkflows(HookRuntime.POST_SIMULATION, ert=ERT.ert )

        self.setPhaseName("Analyzing...")

        EnkfSimulationRunner.runWorkflows(HookRuntime.PRE_UPDATE, ert=ERT.ert )
        es_update = self.ert().getESUpdate( )

        # This line will store new parameters in the target case FS
        success = es_update.smootherUpdate( prior_context )

        if not success:
            raise ErtRunError("Analysis of simulation failed!")
        EnkfSimulationRunner.runWorkflows(HookRuntime.POST_UPDATE, ert=ERT.ert )

        previous_ensemble_name = dump_to_new_storage(reference=None)

        self.setPhase(1, "Running simulations...")
        self.ert().getEnkfFsManager().switchFileSystem( prior_context.get_target_fs( ) )

        self.setPhaseName("Pre processing...")

        rerun_context = self.create_context( arguments, prior_context = prior_context )

        self.ert().getEnkfSimulationRunner().createRunPath( rerun_context )
        EnkfSimulationRunner.runWorkflows(HookRuntime.PRE_SIMULATION, ert=ERT.ert )

        self.setPhaseName("Running forecast...", indeterminate=False)

        self._job_queue = self._queue_config.create_job_queue( )
        num_successful_realizations = self.ert().getEnkfSimulationRunner().runSimpleStep(self._job_queue, rerun_context)

        self.checkHaveSufficientRealizations(num_successful_realizations)

        self.setPhaseName("Post processing...", indeterminate=True)
        EnkfSimulationRunner.runWorkflows(HookRuntime.POST_SIMULATION, ert=ERT.ert)

        self.setPhase(2, "Simulations completed.")

        analysis_module_name = self.ert().analysisConfig().activeModuleName()
        dump_to_new_storage(reference=(previous_ensemble_name, analysis_module_name))

        return prior_context


    def create_context(self, arguments, prior_context = None):

        model_config = self.ert().getModelConfig( )
        runpath_fmt = model_config.getRunpathFormat( )
        jobname_fmt = model_config.getJobnameFormat( )
        subst_list = self.ert().getDataKW( )
        fs_manager = self.ert().getEnkfFsManager()
        if prior_context is None:
            sim_fs = fs_manager.getCurrentFileSystem( )
            target_fs = fs_manager.getFileSystem(arguments["target_case"])
            itr = 0
            mask = arguments["active_realizations"]
        else:
            itr = 1
            sim_fs = prior_context.get_target_fs( )
            target_fs = None
            state = RealizationStateEnum.STATE_HAS_DATA | RealizationStateEnum.STATE_INITIALIZED
            mask = sim_fs.getStateMap().createMask(state)

        # Deleting a run_context removes the possibility to retrospectively
        # determine detailed progress. Thus, before deletion, the detailed
        # progress is stored.
        self.updateDetailedProgress()

        run_context = ErtRunContext.ensemble_smoother( sim_fs, target_fs, mask, runpath_fmt, jobname_fmt, subst_list, itr)
        self._run_context = run_context
        self._last_run_iteration = run_context.get_iter()
        return run_context

    @classmethod
    def name(cls):
        return "Ensemble Smoother"
