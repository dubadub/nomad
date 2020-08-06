package scheduler

import (
	"fmt"
	"time"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/helper/uuid"
	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	// maxSystemDeploymentScheduleAttempts is used to limit the number of times
	// we will attempt to schedule if we continue to hit conflicts for system
	// jobs.
	maxSystemDeploymentScheduleAttempts = 5
)

// SystemOnDeploymentScheduler is used for 'system' jobs. This scheduler is
// designed for services that should be run on every client.
// One for each job, containing an allocation for each node
type SystemOnDeploymentScheduler struct {
	logger  log.Logger
	state   State
	planner Planner

	eval       *structs.Evaluation
	job        *structs.Job
	plan       *structs.Plan
	planResult *structs.PlanResult
	ctx        *EvalContext
	stack      *SystemStack
	nodes      []*structs.Node
	nodesByDC  map[string]int

	followUpEvals []*structs.Evaluation

	deployment *structs.Deployment

	blocked        *structs.Evaluation
	failedTGAllocs map[string]*structs.AllocMetric
	queuedAllocs   map[string]int
}

// NewSystemOnDeploymentScheduler is a factory function to instantiate a new system
// scheduler.
func NewSystemOnDeploymentScheduler(logger log.Logger, state State, planner Planner) Scheduler {
	return &SystemOnDeploymentScheduler{
		logger:  logger.Named("system_sched"),
		state:   state,
		planner: planner,
	}
}

// Process is used to handle a single evaluation.
func (s *SystemOnDeploymentScheduler) Process(eval *structs.Evaluation) error {
	// Store the evaluation
	s.eval = eval

	// Update our logger with the eval's information
	s.logger = s.logger.With("eval_id", eval.ID, "job_id", eval.JobID, "namespace", eval.Namespace)

	// Verify the evaluation trigger reason is understood
	switch eval.TriggeredBy {
	case structs.EvalTriggerJobRegister, structs.EvalTriggerJobDeregister,
		structs.EvalTriggerNodeDrain, structs.EvalTriggerNodeUpdate,
		structs.EvalTriggerAllocStop,
		structs.EvalTriggerRollingUpdate, structs.EvalTriggerQueuedAllocs,
		structs.EvalTriggerPeriodicJob, structs.EvalTriggerMaxPlans,
		structs.EvalTriggerDeploymentWatcher, structs.EvalTriggerRetryFailedAlloc,
		structs.EvalTriggerFailedFollowUp, structs.EvalTriggerPreemption,
		structs.EvalTriggerScaling:
	default:
		desc := fmt.Sprintf("scheduler cannot handle '%s' evaluation reason",
			eval.TriggeredBy)
		return setStatus(s.logger, s.planner, s.eval, nil, s.blocked,
			s.failedTGAllocs, structs.EvalStatusFailed, desc, s.queuedAllocs,
			s.deployment.GetID())
	}

	// Retry up to the maxScheduleAttempts and reset if progress is made.
	progress := func() bool { return progressMade(s.planResult) }
	limit := maxSystemDeploymentScheduleAttempts

	if err := retryMax(limit, s.process, progress); err != nil {
		if statusErr, ok := err.(*SetStatusError); ok {
			// Scheduling was tried but made no forward progress so create a
			// blocked eval to retry once resources become available.
			var mErr multierror.Error
			if err := s.createBlockedEval(true); err != nil {
				mErr.Errors = append(mErr.Errors, err)
			}
			if err := setStatus(s.logger, s.planner, s.eval, nil, s.blocked,
				s.failedTGAllocs, statusErr.EvalStatus, err.Error(),
				s.queuedAllocs, s.deployment.GetID()); err != nil {
				mErr.Errors = append(mErr.Errors, err)
			}
			return mErr.ErrorOrNil()
		}
		return err
	}

	// If the current evaluation is a blocked evaluation and we didn't place
	// everything, do not update the status to complete.
	if s.eval.Status == structs.EvalStatusBlocked && len(s.failedTGAllocs) != 0 {
		e := s.ctx.Eligibility()
		newEval := s.eval.Copy()
		newEval.EscapedComputedClass = e.HasEscaped()
		newEval.ClassEligibility = e.GetClasses()
		newEval.QuotaLimitReached = e.QuotaLimitReached()
		return s.planner.ReblockEval(newEval)
	}

	// Update the status to complete
	return setStatus(s.logger, s.planner, s.eval, nil, s.blocked,
		s.failedTGAllocs, structs.EvalStatusComplete, "", s.queuedAllocs,
		s.deployment.GetID())
}


// createBlockedEval creates a blocked eval and submits it to the planner. If
// failure is set to true, the eval's trigger reason reflects that.
func (s *SystemOnDeploymentScheduler) createBlockedEval(planFailure bool) error {
	e := s.ctx.Eligibility()
	escaped := e.HasEscaped()

	// Only store the eligible classes if the eval hasn't escaped.
	var classEligibility map[string]bool
	if !escaped {
		classEligibility = e.GetClasses()
	}

	s.blocked = s.eval.CreateBlockedEval(classEligibility, escaped, e.QuotaLimitReached())
	if planFailure {
		s.blocked.TriggeredBy = structs.EvalTriggerMaxPlans
		s.blocked.StatusDescription = blockedEvalMaxPlanDesc
	} else {
		s.blocked.StatusDescription = blockedEvalFailedPlacements
	}

	return s.planner.CreateEval(s.blocked)
}

// process is wrapped in retryMax to iteratively run the handler until we have no
// further work or we've made the maximum number of attempts.
func (s *SystemOnDeploymentScheduler) process() (bool, error) {
	// Lookup the Job by ID
	var err error
	ws := memdb.NewWatchSet()
	s.job, err = s.state.JobByID(ws, s.eval.Namespace, s.eval.JobID)
	if err != nil {
		return false, fmt.Errorf("failed to get job %q: %v", s.eval.JobID, err)
	}

	numTaskGroups := 0
	stopped := s.job.Stopped()
	if !stopped {
		numTaskGroups = len(s.job.TaskGroups)
		// Get the ready nodes in the required datacenters
		s.nodes, s.nodesByDC, err = readyNodesInDCs(s.state, s.job.Datacenters)
		if err != nil {
			return false, fmt.Errorf("failed to get ready nodes: %v", err)
		}
	}
	s.queuedAllocs = make(map[string]int, numTaskGroups)
	s.followUpEvals = nil

	// Create a plan
	s.plan = s.eval.MakePlan(s.job)

	// Get any existing deployment
	s.deployment, err = s.state.LatestDeploymentByJobID(ws, s.eval.Namespace, s.eval.JobID)
	if err != nil {
		return false, fmt.Errorf("failed to get job deployment %q: %v", s.eval.JobID, err)
	}

	// Reset the failed allocations
	s.failedTGAllocs = nil

	// Create an evaluation context
	s.ctx = NewEvalContext(s.state, s.plan, s.logger)

	// Construct the placement stack
	s.stack = NewSystemStack(s.ctx)
	if !s.job.Stopped() {
		s.stack.SetJob(s.job)
	}

	// Compute the target job allocations
	if err := s.computeJobAllocs(); err != nil {
		s.logger.Error("failed to compute job allocations", "error", err)
		return false, err
	}

	// If there are failed allocations, we need to create a blocked evaluation
	// to place the failed allocations when resources become available. If the
	// current evaluation is already a blocked eval, we reuse it. If not, submit
	// a new eval to the planner in createBlockedEval. If rescheduling should
	// be delayed, do that instead.
	delayInstead := len(s.followUpEvals) > 0 && s.eval.WaitUntil.IsZero()

	if s.eval.Status != structs.EvalStatusBlocked && len(s.failedTGAllocs) != 0 && s.blocked == nil &&
		!delayInstead {
		if err := s.createBlockedEval(false); err != nil {
			s.logger.Error("failed to make blocked eval", "error", err)
			return false, err
		}
		s.logger.Debug("failed to place all allocations, blocked eval created", "blocked_eval_id", s.blocked.ID)
	}

	// If the plan is a no-op, we can bail. If AnnotatePlan is set submit the plan
	// anyways to get the annotations.
	if s.plan.IsNoOp() && !s.eval.AnnotatePlan {
		return true, nil
	}

	// Create follow up evals for any delayed reschedule eligible allocations, except in
	// the case that this evaluation was already delayed.
	if delayInstead {
		for _, eval := range s.followUpEvals {
			eval.PreviousEval = s.eval.ID
			// TODO(preetha) this should be batching evals before inserting them
			if err := s.planner.CreateEval(eval); err != nil {
				s.logger.Error("failed to make next eval for rescheduling", "error", err)
				return false, err
			}
			s.logger.Debug("found reschedulable allocs, followup eval created", "followup_eval_id", eval.ID)
		}
	}

	// Submit the plan and store the results.
	result, newState, err := s.planner.SubmitPlan(s.plan)
	s.planResult = result
	if err != nil {
		return false, err
	}

	// Decrement the number of allocations pending per task group based on the
	// number of allocations successfully placed
	adjustQueuedAllocations(s.logger, result, s.queuedAllocs)

	// If we got a state refresh, try again since we have stale data
	if newState != nil {
		s.logger.Debug("refresh forced")
		s.state = newState
		return false, nil
	}

	// Try again if the plan was not fully committed, potential conflict
	fullCommit, expected, actual := result.FullCommit(s.plan)
	if !fullCommit {
		s.logger.Debug("plan didn't fully commit", "attempted", expected, "placed", actual)
		if newState == nil {
			return false, fmt.Errorf("missing state refresh after partial commit")
		}
		return false, nil
	}

	// Success!
	return true, nil
}

// computeJobAllocs is used to reconcile differences between the job,
// existing allocations and node status to update the allocations.
func (s *SystemOnDeploymentScheduler) computeJobAllocs() error {
	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	allocs, err := s.state.AllocsByJob(ws, s.eval.Namespace, s.eval.JobID, true)
	if err != nil {
		return fmt.Errorf("failed to get allocs for job '%s': %v",
			s.eval.JobID, err)
	}

	// Determine the tainted nodes containing job allocs
	tainted, err := taintedNodes(s.state, allocs)
	if err != nil {
		return fmt.Errorf("failed to get tainted nodes for job '%s': %v",
			s.eval.JobID, err)
	}

	// Update the allocations which are in pending/running state on tainted
	// nodes to lost, but only if the scheduler has already marked them
	updateNonTerminalAllocsToLost(s.plan, tainted, allocs)

	nodesCount := len(s.nodes)

	reconciler := NewAllocReconciler(s.logger,
		genericAllocUpdateFn(s.ctx, s.stack, s.eval.ID),
		false, s.eval.JobID, s.job, s.deployment, allocs, tainted, s.eval.ID, &nodesCount)
	results := reconciler.Compute()
	s.logger.Debug("reconciled current state with desired state", "results", log.Fmt("%#v", results))

	if s.eval.AnnotatePlan {
		s.plan.Annotations = &structs.PlanAnnotations{
			DesiredTGUpdates: results.desiredTGUpdates,
		}
	}

	// Add the deployment changes to the plan
	s.plan.Deployment = results.deployment
	s.plan.DeploymentUpdates = results.deploymentUpdates

	// Store all the follow up evaluations from rescheduled allocations
	if len(results.desiredFollowupEvals) > 0 {
		for _, evals := range results.desiredFollowupEvals {
			s.followUpEvals = append(s.followUpEvals, evals...)
		}
	}

	// Update the stored deployment
	if results.deployment != nil {
		s.deployment = results.deployment
	}

	// Handle the stop
	for _, stop := range results.stop {
		s.plan.AppendStoppedAlloc(stop.alloc, stop.statusDescription, stop.clientStatus, stop.followupEvalID)
	}

	// Handle the in-place updates
	for _, update := range results.inplaceUpdate {
		if update.DeploymentID != s.deployment.GetID() {
			update.DeploymentID = s.deployment.GetID()
			update.DeploymentStatus = nil
		}
		s.ctx.Plan().AppendAlloc(update)
	}

	// Handle the annotation updates
	for _, update := range results.attributeUpdates {
		s.ctx.Plan().AppendAlloc(update)
	}

	// Nothing remaining to do if placement is not required
	if len(results.place)+len(results.destructiveUpdate) == 0 {
		// If the job has been purged we don't have access to the job. Otherwise
		// set the queued allocs to zero. This is true if the job is being
		// stopped as well.
		if s.job != nil {
			for _, tg := range s.job.TaskGroups {
				s.queuedAllocs[tg.Name] = 0
			}
		}
		return nil
	}

	// Record the number of allocations that needs to be placed per Task Group
	for _, place := range results.place {
		s.queuedAllocs[place.taskGroup.Name] += 1
	}
	for _, destructive := range results.destructiveUpdate {
		s.queuedAllocs[destructive.placeTaskGroup.Name] += 1
	}

	// Compute the placements
	place := make([]placementResult, 0, len(results.place))
	for _, p := range results.place {
		place = append(place, p)
	}

	destructive := make([]placementResult, 0, len(results.destructiveUpdate))
	for _, p := range results.destructiveUpdate {
		destructive = append(destructive, p)
	}
	return s.computePlacements(destructive, place)
}

// computePlacements computes placements for allocations
func (s *SystemOnDeploymentScheduler) computePlacements(destructive, place []placementResult) error {
	nodeIDs := make([]string, len(s.nodes))
	nodeByID := make(map[string]*structs.Node, len(s.nodes))

	for index, node := range s.nodes {
		nodeByID[node.ID] = node
		nodeIDs[index] = node.ID
	}

	var deploymentID string
	if s.deployment != nil && s.deployment.Active() {
		deploymentID = s.deployment.ID
	}

	// Update the set of placement nodes
	nodes := make([]*structs.Node, 1)

	// Capture current time to use as the start time for any rescheduled allocations
	now := time.Now()

	curr := 0

	// Have to handle destructive changes first as we need to discount their
	// resources. To understand this imagine the resources were reduced and the
	// count was scaled up.

	for _, results := range [][]placementResult{destructive, place} {
		for _, missing := range results {
			// Get the task group
			tg := missing.TaskGroup()


			var node *structs.Node

			prevAllocation := missing.PreviousAllocation()

			if prevAllocation != nil{
				node = nodeByID[prevAllocation.NodeID]

				if node != nil {
					s.logger.Error("failed to find node of previos allocation", "prevAllocationID", prevAllocation.ID,  "nodeID", prevAllocation.NodeID)
				}
			} else {
				for node == nil && curr < len(nodeIDs) {
					node = nodeByID[nodeIDs[curr]]
					curr += 1
				}
			}

			nodeByID[node.ID] = nil


			stopPrevAlloc, stopPrevAllocDesc := missing.StopPreviousAlloc()

			if stopPrevAlloc {
				s.plan.AppendStoppedAlloc(prevAllocation, stopPrevAllocDesc, "", "")
			}

			// Update the set of placement nodes
			nodes[0] = node
			s.stack.SetNodes(nodes)

			// Attempt to match the task group
			option := s.stack.Select(tg, nil)

			// Store the available nodes by datacenter
			s.ctx.Metrics().NodesAvailable = s.nodesByDC

			// Compute top K scoring node metadata
			s.ctx.Metrics().PopulateScoreMetaData()

			// Set fields based on if we found an allocation option
			if option != nil {
				resources := &structs.AllocatedResources{
					Tasks:          option.TaskResources,
					TaskLifecycles: option.TaskLifecycles,
					Shared: structs.AllocatedSharedResources{
						DiskMB: int64(tg.EphemeralDisk.SizeMB),
					},
				}
				if option.AllocResources != nil {
					resources.Shared.Networks = option.AllocResources.Networks
					resources.Shared.Ports = option.AllocResources.Ports
				}

				// Create an allocation for this
				alloc := &structs.Allocation{
					ID:                 uuid.Generate(),
					Namespace:          s.job.Namespace,
					EvalID:             s.eval.ID,
					Name:               missing.Name(),
					JobID:              s.job.ID,
					TaskGroup:          tg.Name,
					Metrics:            s.ctx.Metrics(),
					NodeID:             option.Node.ID,
					NodeName:           option.Node.Name,
					DeploymentID:       deploymentID,
					TaskResources:      resources.OldTaskResources(),
					AllocatedResources: resources,
					DesiredStatus:      structs.AllocDesiredStatusRun,
					ClientStatus:       structs.AllocClientStatusPending,
					// SharedResources is considered deprecated, will be removed in 0.11.
					// It is only set for compat reasons.
					SharedResources: &structs.Resources{
						DiskMB:   tg.EphemeralDisk.SizeMB,
						Networks: resources.Shared.Networks,
					},
				}

				// If the new allocation is replacing an older allocation then we
				// set the record the older allocation id so that they are chained
				if prevAllocation != nil {
					alloc.PreviousAllocation = prevAllocation.ID
					if missing.IsRescheduling() {
						updateRescheduleTracker(alloc, prevAllocation, now)
					}
				}

				// If we are placing a canary and we found a match, add the canary
				// to the deployment state object and mark it as a canary.
				if missing.Canary() && s.deployment != nil {
					alloc.DeploymentStatus = &structs.AllocDeploymentStatus{
						Canary: true,
					}
				}

				s.handlePreemptions(option, alloc, missing)

				// Track the placement
				s.plan.AppendAlloc(alloc)

			} else {
				// Lazy initialize the failed map
				if s.failedTGAllocs == nil {
					s.failedTGAllocs = make(map[string]*structs.AllocMetric)
				}

				// Track the fact that we didn't find a placement
				s.failedTGAllocs[tg.Name] = s.ctx.Metrics()

				// If we weren't able to find a replacement for the allocation, back
				// out the fact that we asked to stop the allocation.
				if stopPrevAlloc {
					s.plan.PopUpdate(prevAllocation)
				}
			}

		}
	}

	return nil
}


// handlePreemptions sets relevant preeemption related fields.
func (s *SystemOnDeploymentScheduler) handlePreemptions(option *RankedNode, alloc *structs.Allocation, missing placementResult) {
	if option.PreemptedAllocs == nil {
		return
	}

	// If this placement involves preemption, set DesiredState to evict for those allocations
	var preemptedAllocIDs []string
	for _, stop := range option.PreemptedAllocs {
		s.plan.AppendPreemptedAlloc(stop, alloc.ID)
		preemptedAllocIDs = append(preemptedAllocIDs, stop.ID)

		if s.eval.AnnotatePlan && s.plan.Annotations != nil {
			s.plan.Annotations.PreemptedAllocs = append(s.plan.Annotations.PreemptedAllocs, stop.Stub())
			if s.plan.Annotations.DesiredTGUpdates != nil {
				desired := s.plan.Annotations.DesiredTGUpdates[missing.TaskGroup().Name]
				desired.Preemptions += 1
			}
		}
	}

	alloc.PreemptedAllocations = preemptedAllocIDs
}
