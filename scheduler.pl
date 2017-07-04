%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%             scheduler.pl              %
%                                       %
%     Declarative Programming Project   %
%       Task-Parallel Scheduler         %
%                                       %
%             Nicolas Omer              %
%                                       %
%       Declarative Programming         %
%              2014-2015                %
%                                       %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% main
%   Entry point of the program

main(Deadline) :-
    writeln('Starting whole computation...'), nl,
    sample(StartTime),
    mainOptimal,
    mainHeuristic(Deadline),
    sample(EndTime),
    printTime('Total Program runtime : ', StartTime, EndTime),
    !.

mainOptimal :-
    writeln('Finding all optimal solutions...'), nl, 
    sample(StartTime),
    consultBSHO,
    optimalPrint, 
    consultBSHE,
    optimalPrint,
    consultFSN,
    optimalPrint,
    consultFSU,
    optimalPrint,
    consultSS,
    optimalPrint,
    sample(EndTime),
    printTime('Find all optimal solutions took : ', StartTime, EndTime).

mainHeuristic(Deadline) :-
    writeln('Find all heuristic solutions...'), nl, 
    sample(StartTime),
    consultBLHO,
    heuristicPrint(Deadline),
    consultBLHE,
    heuristicPrint(Deadline), 
    consultFLN,
    heuristicPrint(Deadline), 
    consultFLU,
    heuristicPrint(Deadline), 
    consultSL,
    heuristicPrint(Deadline),
    sample(EndTime),
    printTime('Find all heuristic solutions took : ', StartTime, EndTime).

% isSolution(+S:Solution).
%   Checks whether an execution schedule Solution is valid.

isSolution(solution(ScheduleList)) :- 
    listTasks(TasksList),
    isSolution(TasksList, ScheduleList), 
    !.

% isSolution(+TL:TasksList, -SL:ScheduleList).
%   Checks whether TasksList can be used to build a ScheduleList.

isSolution([], ScheduleList) :- 
    listCores(CoresList), 
    findall(Core, member(schedule(Core, _), ScheduleList), Cores), 
    equalLists(Cores, CoresList).
    
isSolution(TasksList, ScheduleList) :- 
    findAvailableTuples(TasksList, ScheduleList, AvailableTuples),
    filterTasks(AvailableTuples, TasksList, NewTasksList),
    isSolution(NewTasksList, ScheduleList).
 
% findAvailableTuples(+TTL:TodoTasksList, +SL:ScheduleList, ?AT:AvailableTuples).
%   Where TodoTasksList is a list of tasks, that still need to be scheduled.
%   ScheduleList is a list of schedules -> schedule(Core, Tasks).
%   AvailableTuples is a list of Tuples (Task, Core) representing 
%   the Tasks from TodoTasksList that can be executed now. 

findAvailableTuples(TodoTasksList, ScheduleList, AvailableTuples) :- 
    findall( (Task, Core), 
    (
        core(Core),
        member(Task, TodoTasksList), 
        not( (depends_on(Task, T, _), member(T, TodoTasksList)) ),
        firstNonExecutedCore(ScheduleList, TodoTasksList, (Task, Core))
    )
    , AvailableTuples).

% firstNonExecutedCore(+SL:SolutionList, +T:Todo, -TCT:TaskCoreTuple). 
%   True iff the given task is the first non executed task of its Core in the given solution.

firstNonExecutedCore([schedule(Core, TasksList) | _], Todo, (Task, Core)) :- 
    !,
    member(Task, TasksList),
    firstNonExecutedTaskList(TasksList, Todo, Task).
    
firstNonExecutedCore([_ | TasksListRest], Todo, (Task, Core)) :-
    firstNonExecutedCore(TasksListRest, Todo, (Task, Core)).

% firstNonExecutedTaskList(+TL:TasksList, +T:Todo, -T:Task).

firstNonExecutedTaskList([T | _ ], _, T).
firstNonExecutedTaskList([HeadTasksList | RestTasksList], Todo, T) :- 
    not(member(HeadTasksList, Todo)),
    firstNonExecutedTaskList(RestTasksList, Todo, T).

% prevTaskOnCoreEndTime(+SL:SolutionList, +PCWC:ProcessCostWithCore(TripleList), +T:Task, +C:Core, -PT:PreviousTime).    
%   Find the end time of the previous task on the same core in the solution.

prevTaskOnCoreEndTime([schedule(Core, TasksList) | _], PCWC, Task, Core, Prev) :-
    prevTaskInListEndTime(TasksList, PCWC, Task, Prev).
prevTaskOnCoreEndTime([_ | RestTasksList], PCWC, Task, Core, Prev) :-
    prevTaskOnCoreEndTime(RestTasksList, PCWC, Task, Core, Prev).

% prevTaskInListEndTime(+TL:TasksList, +PCWC:ProcessCostWithCore(TripleList), +T:Task, -PT:PreviousTime).

prevTaskInListEndTime([HeadTasksList | _], _, HeadTasksList, 0).
prevTaskInListEndTime([HeadTasksList | RestTasksList], PCWC, Task, PreviousTime) :- 
    [Task | _ ] = RestTasksList,
    !,
    findTaskInPCWC(PCWC, HeadTasksList, (_, _, PreviousTime)).
prevTaskInListEndTime([_ | RestTasksList], PCWC, Task, PreviousTime) :- 
    prevTaskInListEndTime(RestTasksList, PCWC, Task, PreviousTime).  

% taskEndTime(+TCT:TaskCoreTuple, +SL:SolutionList, +PCWC:ProcessCostWithCore(TripleList), -ST:StartTime).
%   Determine the earliest StartTime of a Task using the TaskCoreTuple and ProcessCostWithCore(TripleList).

taskStartTime((Task, Core), SolutionList, PCWC, StartTime) :-
    prevTaskOnCoreEndTime(SolutionList, PCWC, Task, Core, PrevTime),
    findall(Time, 
    (
        depends_on(Task, X, Cost),
        findTaskInPCWC(PCWC, X, (X, XCore, XTime)),
        channel(Core, XCore, Lat, Band),
        ( (Core \= XCore)
            -> Time is (Lat + XTime + Cost / Band)
            ;  Time is (XTime)
        )
    ),
    DepTimes),
    append([PrevTime], DepTimes, Times),
    max_list(Times, StartTime).
 
% taskEndTime(+TCT:TaskCoreTuple, +SL:SolutionList, +PCWC:ProcessCostWithCore(TripleList), -ET:EndTime).
%   Computes the EndTime of a Task on the Core using the TaskCoreTuple and ProcessCostWithCore(TripleList).

taskEndTime((Task, Core), SolutionList, PCWC, EndTime) :-
    taskStartTime((Task, Core), SolutionList, PCWC, StartTime),
    process_cost(Task, Core, Cost),
    EndTime is (StartTime + Cost).

% addAllEndTimes(+TCT:TaskCoreTuple, +SL:ScheduleList, +PCWC, NPCWC:NewPCWC, T:Todo, NT:NewTodo).
%   Add all given (Task, Core) to the given execution with their 
%   corresponding finish time, also updates the list of tasks todo.

addAllEndTimes([], _, PCWC, PCWC, Todo, Todo).
addAllEndTimes([(Task, Core) | RestTaskCoreTuple], ScheduleList, PCWC, NewPCWC, Todo, NewTodo) :-
    taskEndTime((Task, Core), ScheduleList, PCWC, EndTime),
    addToPCWC(PCWC, (Task, Core, EndTime), PCWCTemp),
    delete(Todo, Task, NTodo),
    addAllEndTimes(RestTaskCoreTuple, ScheduleList, PCWCTemp, NewPCWC, NTodo, NewTodo).

% execution_time_sequential(+SL:SolutionsList, +TL:TimeList, -ET1:ExecutionTime).
%   Given a SolutionsList with the trivial single core schedules and the empty list
%   as accumulator for TimeList, outputs the minimum ExecutionTime of that SolutionsList.

execution_time_sequential(SolutionsList, ExecutionTime) :- execution_time_sequential(SolutionsList,  [], ExecutionTime), !.

execution_time_sequential([], TimeList, ET1) :- min_list(TimeList, ET1).
execution_time_sequential([HeadSolutionsList | RestSolutionsList], TimeList, ET1) :-
    timeOfSolution(HeadSolutionsList, SolutionTime),
    append([SolutionTime], TimeList, NewTimeList),
    execution_time_sequential(RestSolutionsList, NewTimeList, ET1).

% timeOfSolution(+S:Solution, +TAcc:TimeList, -ET1:ExecutionTime).
%   Outputs the ExecutionTime of a single Solution starting with an empty TimeList.

% Start with zero as accumulator.

timeOfSolution(Solution, ET1) :- timeOfSolution(Solution, [], ET1).

timeOfSolution(solution([]), TimeList, SolutionTime) :- max_list(TimeList, SolutionTime).
timeOfSolution(solution([HeadSchedulesList | RestSchedulesList]), TimeList, SolutionTime) :-
    timeOfSchedule(HeadSchedulesList, TimeOfSchedule),
    append([TimeOfSchedule], TimeList, NewTimeList),
    timeOfSolution(solution(RestSchedulesList), NewTimeList, SolutionTime).

% timeOfSchedule(+Sch:Schedule(Core, Tasks), +TAcc:TimeAccumulator, -ST:ScheduleTime).
%   Outputs the ScheduleTime of a single Schedule starting with an empty accumulator TimeAccumulator.

% Start with zero as accumulator.

timeOfSchedule(Schedule, ScheduleTime) :- 
    timeOfSchedule(Schedule, 0, ScheduleTime).

timeOfSchedule(schedule(_, []), ScheduleTime, ScheduleTime).
timeOfSchedule(schedule(Core, [HeadTasksList | RestTasksList]), Acc, ScheduleTime) :-
    process_cost(HeadTasksList, Core, Cost),
    NewAcc is Acc + Cost,
    timeOfSchedule(schedule(Core, RestTasksList), NewAcc, ScheduleTime).

% execution_time(+S:Solution, ?ET:ExecutionTime).
%   Where ET is the execution time of Solution.
%   Execution time is defined as the latest 
%   completion time of any task in the schedule.

execution_time(solution(ScheduleList), ET) :- 
    listTasks(TodoTasksList),
    execution_time(TodoTasksList, ScheduleList, processCostWithCore([]), ET), 
    !.

% execution_time(+TTL:TodoTasksList, +SL:ScheduleList, +PCWC:processCostWithCore(TriplesList), ?ET:ExecutionTime).
%   Where TodoTasksList is a list of tasks, that still need to be scheduled.
%   ScheduleList is a list of schedules -> schedule(Core, Tasks).
%   processCostWithCore(TriplesList) contains a list of triples 
%   process_cost(Task, Core, Cost) where the Core is assigned.
%   And outputs the ExecutionTime of the ScheduleList.

execution_time([], _, processCostWithCore([]), 0).
    
execution_time([], _, processCostWithCore(L), ET) :- 
    findall(Time, member((_, _, Time), L), Times),
    max_list(Times, ET).

execution_time(TodoTasksList, ScheduleList, PCWC, ET) :- 
    findAvailableTuples(TodoTasksList, ScheduleList, AvailableTuples),
    addAllEndTimes(AvailableTuples, ScheduleList, PCWC, NewPCWC, TodoTasksList, NewTodoTasksList),
    execution_time(NewTodoTasksList, ScheduleList, NewPCWC, ET).

% addToPCWC(+PCWC:processCostWithCore(TriplesList), +T:Triple, -NTL:NewTriplesList).
%   Add Triple : (Task, Core, Cost) to the TriplesList of PCWC.

addToPCWC(processCostWithCore(TriplesList), (Task, Core, Cost), processCostWithCore(NewTriplesList)) :-
    append([(Task, Core, Cost)], TriplesList, NewTriplesList).

% findTaskInPCWC(+PCWC:processCostWithCore(TriplesList), +T:Task, -T:Triple).
%   Allow to find a Task in the processCostWithCore structure 
%   and ouputs the corresponding Triple it belongs to.

findTaskInPCWC(processCostWithCore([]), _, 0).
findTaskInPCWC(processCostWithCore([ (Task, Core, Cost) | _]), Task, (Task, Core, Cost)).
findTaskInPCWC(processCostWithCore([_ | Tail]), Task, Res) :-
    findTaskInPCWC(processCostWithCore(Tail), Task, Res).

% find_heuristically(-S:Solution).
%   Finds an execution schedule S, approximately minimizing execution time.

% By default, 3 seconds to improve heuristic solution

find_heuristically(Solution) :- find_heuristically(Solution, 3).

find_heuristically(Solution, DeadLine) :- 
    find_optimal_sequential(SingleCoreSolution),
    randomPermutations(SingleCoreSolution, DeadLine, Solution),
    !.

% find_optimal_sequential(-S:Solution).
%   Finds an execution schedule Solution, with all tasks 
%   running on a single core, minimizing execution time.

find_optimal_sequential(Solution) :- 
    initSolution(EmptySolutionsList),
    listCores(CoresList),
    listTasks(TasksList),
    createSingleCoreSolutions(CoresList, TasksList, EmptySolutionsList, SolutionsList),
    execution_time_sequential(SolutionsList, ET1),
    member(Solution, SolutionsList),
    timeOfSolution(Solution, ET1).

% speedup(+S:Solution, -S:Speedup).
%   The Speedup of a schedule Solution is given by the ratio ET1/ET(S).

speedup(Solution, Speedup) :-
    writeln('Computing Speedup factor...'),
    execution_time(Solution, ET),
    find_optimal_sequential(SequentialSolution),
    timeOfSolution(SequentialSolution, ET1),
    Speedup is (ET1 / ET), !.

% find_optimal(-S:Solution).
%   Finds an execution schedule Solution, exactly minimizing execution time.

find_optimal(Solution) :- 
    initSolution(EmptySolutionsList),
    listTasks(TasksList),
    optimal(TasksList, [(EmptySolutionsList, processCostWithCore([]))], Solution).

% optimal(+TTL:TodoTasksList, +SPCWCL:SolutionsPCWCList, -S:Solution).
%   Where TodoTasksList is a list of tasks, that still need to be scheduled.
%   SolutionsPCWCList is a list with tuples (SolutionsList, processCostWithCore(TriplesList)),
%   so that a Solution in the SolutionsList have a list of corresponding 
%   Triples (Task, Core, Cost) where every variable is bound.
%   Outputs the execution schedule Solution.

optimal([], SolutionsPCWCList, Solution) :-
    maplist(discardPCWC, SolutionsPCWCList, SolutionsList),
    aggregate(min(ET, X), (member(X, SolutionsList), isSolution(X), execution_time(X, ET)), min(_, Solution)).

optimal(TodoTasksList, SolutionsPCWCList, Solution) :-
    findAvailableTasks(TodoTasksList, AvailableTasksList),
    [CurrentTask | _] = AvailableTasksList,
    delete(TodoTasksList, CurrentTask, NewTodoTasksList),
    addTaskToAllSols(CurrentTask, SolutionsPCWCList, NewSolutionsPCWCList),
    optimal(NewTodoTasksList, NewSolutionsPCWCList, Solution).

% discardPCWC(+SPCWCL:SolutionsPCWCList, -SL:SolutionsList)
%   Since SolutionsPCWCList is a list of (SolutionsList, processCostWithCore(TriplesList)),
%   Discard the processCostWithCore part.

discardPCWC(SolutionsPCWCList, SolutionsList) :-
    (SolutionsList, _) = SolutionsPCWCList.

% initSolution(-ESL:EmptySolutionsList)
%   Creates a EmptySolutionsList : solution(schedule(c1, []), schedule(c2, []), etc...)

initSolution(EmptySolutionsList) :-
    listCores(CoresList),
    EmptySolutionsList = solution(SchedulesList),
    maplist(initSchedule, CoresList, SchedulesList).

% initSchedule(+C:Core, -Sch:Schedule)
%   Creates a empty Schedule : schedule(Core, [])

initSchedule(Core, Schedule) :-
    Schedule = schedule(Core, []).

% findAvailableTasks(+TTL:TodoTasksList, ?ATL:AvailableTasksList)
%   TodoTasksList is a list of tasks, that still need to be schedule.
%   AvailableTasksList is a list of tasks representing the tasks from 
%   TodoTasksList that can be scheduled now, since all depending tasks are done.

findAvailableTasks(TodoTasksList, AvailableTasksList) :- 
    findall( AvailableTask, 
    (
        member(AvailableTask, TodoTasksList), 
        not((depends_on(AvailableTask, Task, _), member(Task, TodoTasksList)))
    ), 
    AvailableTasksList).
 
% addTaskToAllSols(+CT:CurrentTask, +SPCWCL:SolutionsPCWCList, -NSPCSL:NewSolutionsPCWCList).
%   From SolutionsPCWCList which is a list of (solution, processCostWithCore(TripleList)),
%   creates a NewSolutionsPCWCList such that the CurrentTask was added to 
%   all the solutions, at the end of the task list of each core.

addTaskToAllSols(CurrentTask, SolutionsPCWCList, NewSolutionsPCWCList) :- 
    listCores(CoresList),
    addTaskToAllSols(CoresList, CurrentTask, SolutionsPCWCList, [], NewSolutionsPCWCList).
    
% addTaskToAllSols(+CL:CoresList, +CT:CurrentTask, +SPCWCL:SolutionsPCWCList, +Acc:Accumulator, -NSPCSL:NewSolutionsPCWCList).

% Start with an empty list as accumulator.

addTaskToAllSols(CoresList, CurrentTask, SolutionsPCWCList, NewSolutionsPCWCList) :-
    addTaskToAllSols(CoresList, CurrentTask, SolutionsPCWCList, [], NewSolutionsPCWCList).

addTaskToAllSols([], _, _, NewSolutionsPCWCList, NewSolutionsPCWCList).
addTaskToAllSols([HeadCoresList | RestCoresList], CurrentTask, SolutionsPCWCList, AccRes, Res) :-  
    addTasktoCoreLists(CurrentTask, HeadCoresList, SolutionsPCWCList, NewSolutionsPCWCList),
    append(NewSolutionsPCWCList, AccRes, NewAcc),
    addTaskToAllSols(RestCoresList, CurrentTask, SolutionsPCWCList, NewAcc, Res).

% addTasktoCoreLists(+CT:CurrentTask, +C:Core, +SPCWCL:SolutionsPCWCList, -NSPCSL:NewSolutionsPCWCList).

addTasktoCoreLists(CurrentTask, Core, SolutionsPCWCList, NewSolutionsPCWCList) :-
    maplist(addTasktoCoreSol(CurrentTask, Core), SolutionsPCWCList, NewSolutionsPCWCList).

% addTasktoCoreSol(+CT:CurrentTask, +C:Core, +SPCWCL:SolutionsPCWCList, -NSPCSL:NewSolutionsPCWCList).
    
addTasktoCoreSol(CurrentTask, Core, (solution(ScheduleList), PCWC), (solution(NewScheduleList), NewPCWC)) :-
    maplist(addTasktoCore(Core, CurrentTask), ScheduleList, NewScheduleList),
    addAllEndTimes([(CurrentTask, Core)], NewScheduleList, PCWC, NewPCWC, [CurrentTask], _).

% addTasktoCore(+C:Core, +CT:CurrentTask, +OS:OldSchedule, -NS:NewSchedule).
%   Replaces the OldSchedule of the given Core with a NewSchedule
%   that has the CurrentTask at the end of its TasksList.

addTasktoCore(Core, CurrentTask, schedule(Core, TasksList), NewSchedule) :- 
    !,
    append(TasksList, [CurrentTask], NewTasksList),
    NewSchedule = schedule(Core, NewTasksList).

addTasktoCore(_, _, Schedule, Schedule).

% createSingleCoreSolutions(+CL:CoreList, +TL:TaskList, +ESL:EmptySolution, +Acc:Accumulator, -SCS:SingleCoreSolutionsList).
%   Receives the CoreList, TaskList and an EmptySolution to build a SolutionList where all Tasks are running on a single Core.

% Start with an empty list as accumulator.

createSingleCoreSolutions(CoreList, TaskList, EmptySolution, SingleCoreSolutionsList) :-
    createSingleCoreSolutions(CoreList, TaskList, EmptySolution, [], SingleCoreSolutionsList).

createSingleCoreSolutions([], _, _, SingleCoreSolutionsList, SingleCoreSolutionsList).
createSingleCoreSolutions([HeadCoresList | RestCoresList], TasksList, EmptySolution, Acc, SingleCoreSolutionsList) :-
    addAllTasksToCore(HeadCoresList, TasksList, EmptySolution, SingleCoreSolution),
    append([SingleCoreSolution], Acc, NewAcc),
    createSingleCoreSolutions(RestCoresList, TasksList, EmptySolution, NewAcc, SingleCoreSolutionsList).

% addAllTasksToCore(+C:Core, +TL:TasksList, +ES:EmptySolution, -SCS:SingleCoreSolution).
%   Fills the schedules of an EmptySolution to build a SingleCoreSolution.

addAllTasksToCore(Core, TasksList, solution(ScheduleList), solution(NewScheduleList)) :-
    maplist(assignNewSchedule(Core, TasksList), ScheduleList, NewScheduleList).

% assignNewSchedule(+C:Core, +TL:TasksList, +OS:OldSchedule, -NS:NewSchedule).
%   Replaces the OldSchedule of the given Core with a
%   NewSchedule that has all the tasks in the TasksList.

assignNewSchedule(Core, TasksList, schedule(Core, []), NewSchedule) :-
    !,
    NewSchedule = schedule(Core, TasksList).

assignNewSchedule(_, _, Schedule, Schedule).

% randomPermutations(+S:Solution, +DL:DeadLine, -BS:BetterSolution). 
%   Starting with a Single Core Solution, performs random permutations of tasks 
%   (while respecting the dependencies) until DeadLine (in seconds) is reached,
%   returns possibly a BetterSolution with a better execution time.

randomPermutations(Solution, DeadLine, BetterSolution) :-
    sample(StartTime),
    solToRawSol(Solution, Raw),
    writeln('Starting random permutations...'), nl,
    randomPermutations(Solution, Raw, StartTime, DeadLine*1000, BetterSolution).

randomPermutations(BestSolution, _, StartTime, DeadLine, BestSolution) :-
    sample(CurrentTime),
    TotalTime is (CurrentTime - StartTime),
    TotalTime >= DeadLine,
    !, 
    writeln('DeadLine Reached !').

randomPermutations(BestSolution, rawSolution(TaskCoreTuple), StartTime, DeadLine, Final) :- 
    listCores(CoresList),
    length(CoresList, LengthCoresList),
    random_between(1, LengthCoresList, CoreIndex),
    nth1(CoreIndex, CoresList, RandomCore),
    length(TaskCoreTuple, LengthTasksList),
    random_between(1, LengthTasksList, TaskIndex),
    changeRaw(TaskIndex, TaskCoreTuple, [], RandomCore, NewTaskCoreTuple),
    timeOfSolution(BestSolution, BestTime),
    rawSolToSol(rawSolution(NewTaskCoreTuple), CurrentSolution),
    timeOfSolution(CurrentSolution, CurrentTime),
    ( (CurrentTime < BestTime)
        -> NewBestSolution = CurrentSolution
        ;  NewBestSolution = BestSolution
    ),
    randomPermutations(NewBestSolution, rawSolution(NewTaskCoreTuple), StartTime, DeadLine, Final).

% changeRaw(+TI:TaskIndex, +TCT:TaskCoreTuple, +F:Front, +RC:RandomCore, -NTCT:NewTaskCoreTuple).

changeRaw(1, [(Task, _) | TaskCoreTupleRest], Front, NewCore, NewTaskCoreTuple) :-
    append(Front, [(Task, NewCore) | TaskCoreTupleRest], NewTaskCoreTuple).
    
changeRaw(TaskIndex, [TaskCoreTupleHead |TaskCoreTupleRest], Front, NewCore, NewTaskCoreTuple) :-
    append(Front, [TaskCoreTupleHead], NewFront),
    NewIndex is TaskIndex - 1,
    changeRaw(NewIndex, TaskCoreTupleRest, NewFront, NewCore, NewTaskCoreTuple).

% solToRawSol(+S:Solution(SolutionList), -RS:RawSolution(Raw)).
%   Changes the given Solution to a RawSolution.

solToRawSol(solution(SolutionList), rawSolution(Raw)) :-
    sortTasks(Sorted),
    maplist(addCore(SolutionList), Sorted, Raw).

% addCore(+S:Solution, +T:Task, -R:Raw).

addCore(Solution, Task, Raw) :-
    exclude(findTaskSchedule(Task), Solution, CoreSchedule),
    [schedule(Core, _) | _] = CoreSchedule,
    Raw = (Task, Core).

% findTaskSchedule(+T:Task, -Sch:schedule(_, TasksList)).
%   Returns the Schedule of a given Task.

findTaskSchedule(Task, schedule(_, TasksList)) :-
    member(Task, TasksList).

% rawSolToSol(-RS:RawSolution(Raw), -S:solution(SolutionList)).
%   Changes the given RawSolution to a Solution.

rawSolToSol(rawSolution(Raw), solution(SolutionList)) :-
    listCores(Cores),
    rawSolToSolCores(Cores, Raw, [], SolutionList).

% rawSolToSolCores(+CL:CoresList, +R:Raw, +Acc:, -SL:SolutionList).

rawSolToSolCores([], _, SolutionList, SolutionList).
rawSolToSolCores([CoresListHead | CoresListRest], Raw, Acc, SolutionList) :-
    findall(Task,
    (
        member((Task, CoresListHead), Raw)
    ), 
    TaskList),
    append(Acc, [schedule(CoresListHead, TaskList)], NewAcc),
    rawSolToSolCores(CoresListRest, Raw, NewAcc, SolutionList).

% sortTasks(-SortedTasksList)
%   Sort the tasks in such a way that if t1 does not depend on t2 (direct 
%   dependence or indirect one), then t1 will be before t2 in the SortedTasksList.

sortTasks(SortedTasksList) :-
    listTasks(TasksList),
    sortTasks(TasksList, SortedTasksList), 
    !.

% Start with an empty list as accumulator.

sortTasks(TasksList, SortedTasksList) :-
    sortTasks(TasksList, [], SortedTasksList).

sortTasks([], SortedTasksList, SortedTasksList) :-
    listTasks(TasksList),
    equalLists(TasksList, SortedTasksList).
    
sortTasks(TodoTasksList, Acc, SortedTasksList) :-
    findAvailableTasks(TodoTasksList, AvailableTasksList),
    not(length(AvailableTasksList, 0)),
    append(Acc, AvailableTasksList, NewAcc),
    filterTasks(AvailableTasksList, TodoTasksList, NewTodoTasksList),
    sortTasks(NewTodoTasksList, NewAcc, SortedTasksList).

% filterTasks(+ATL:AvailableTasksList, +TTL:TodoTasksList, +Acc:Accumulator, -NTTL:NewTodoTasksList).
%   Removes the AvailableTasksList from TodoTasksList starting with an empty Accumulator to form NewTodoTasksList.
%   If the current AvailableTask is not in TodoTasksList, keep TodoTasksList intact.

% Start with an empty list as accumulator.

filterTasks(List1, List2, List3) :- filterTasks(List1, List2, [], List3).

filterTasks([], _, List3, List3).
filterTasks([Head | Rest], List2, Acc, List3) :-
    ( (member(Head, List2))
        -> delete(List2, Head, NewAcc)
        ;  NewAcc = Acc
    ),
    filterTasks(Rest, NewAcc, NewAcc, List3).

%% Print %%

% Finds, prints the optimal solution and the speedup factor.

optimalPrint :-
    sample(StartTime),
    writeln('Computing optimal solution...'),
    find_optimal(Solution),
    speedup(Solution, Speedup),
    pretty_print(Solution),
    write('With Speedup : '),
    writeln(Speedup),
    sample(EndTime),
    printTime('Optimal solution found in : ', StartTime, EndTime).

% Finds, prints the heuristic solution and the speedup factor.

heuristicPrint(DeadLine) :-
    sample(StartTime),
    writeln('Computing heuristic solution...'),
    find_heuristically(Solution, DeadLine),
    speedup(Solution, Speedup),
    printCost(Solution),
    write('With Speedup : '),
    writeln(Speedup),
    sample(EndTime),
    printTime('Heuristic solution found in : ', StartTime, EndTime),
    !.

% pretty_print(+S:Solution).
%   Outputs an execution schedule Solution nicely, in human readable format. 

pretty_print(solution(SchedulesList)) :- 
    nl,
    writeln('Solution found : '), 
    writeln('_________________'), 
    nl, 
    prettyPrintScheduling('--', SchedulesList),
    printCost(solution(SchedulesList)).

% printCost(+S:Solution).
%   Prints the cost of the schedule of Solution.

printCost(Solution) :-
    execution_time(Solution, ET),
    nl, 
    write('Schedule Cost Time : '), 
    write(ET),
    nl.

% printTime(+M:Message, +ST:StartTime, +ET:EndTime).
%   Prints Message and then the time of the interval (EndTime - StartTime).

printTime(Message, StartTime, EndTime) :-     
    TotalTime is (EndTime - StartTime)/1000,
    nl,
    write(Message),
    write(TotalTime),
    writeln(' sec'), nl.

% prettyPrint(+Sep:Separator, +Sch:SchedulesList).
%   Prints the SchedulesList in a human-readable 
%   format with the given Separator.

prettyPrintScheduling(_, []).

prettyPrintScheduling(Separator, [schedule(Core, Tasks) | RestSchedulesList ]) :- 
    write('Schedule found for core '),
    write(Core),
    write(' : '),
    prettyPrintList(Separator, Tasks),
    nl, 
    prettyPrintScheduling(Separator, RestSchedulesList). 

% prettyPrint(+Sep:Separator, +L:List).
%   Prints a List in a human-readable 
%   format with the given Separator.

prettyPrintList(_, []) :-
    write('[]').
    
prettyPrintList(_, [Element]) :- 
    !, 
    write(Element),
    write('.').
    
prettyPrintList(Separator, [HeadList | RestList]) :-
    write(HeadList),
    write(Separator),
    prettyPrintList(Separator, RestList).

%% Helper Functors %%

% listTasks(-L:TaskList) :
%   Lists all given tasks.

listTasks(List) :- findall(Task, task(Task), List).

% listCores(-L:CoreList) :
%   Lists all given cores.

listCores(List) :- findall(Core, core(Core), List).

% equalLists(+L1:List1, +L2:List2).
%   Checks if List1 and List2 have the same elements
%   by saying they must have the same subsets.

equalLists(List1, List2) :-
    isSubset(List1, List2),
    isSubset(List2, List1).

% isSubset(+List1, +List2).
%   Check if List1 is a subset of List2.
%   Where select(?Elem, ?List1, ?List2) is true 
%   when List1, with Elem removed, results in List2.

isSubset([], _).
isSubset([HeadList | RestList], List1) :-
    select(HeadList, List1, List2),
    !,
    isSubset(RestList, List2).

% sample(-CPUT:CPUTime).
%   Returns the CPUTime, where :
%   statistics(+Key, -Value).
%   Unify system statistics determined by Key with Value, only the head is relevant
%   since runtime = [ CPU time in milliseconds, CPU time in milliseconds since ... ].

sample(CPUTime) :-
    statistics(runtime, Val),
    [CPUTime | _] = Val.

% deleteExistingData.
%   The only way to completely remove a predicates clauses and 
%   properties from the system abolish(F, N) : abolish the predicate 
%   named F of arity N,done before every data file consult.

deleteExistingData :- 
    abolish(core/1),
    abolish(task/1),
    abolish(depends_on/3),
    abolish(process_cost/3),
    abolish(channel/4).

% Consult the files of the respecting 10 instances.
 
consultBSHO :-
    deleteExistingData,
    writeln('Consulting batch_small_homo.pl...'),
    consult('batch_small_homo.pl'), nl.

consultBSHE :-
    deleteExistingData,
    writeln('Consulting batch_small_hetero.pl...'),
    consult('batch_small_hetero.pl'), nl.
    
consultFSU :-
    deleteExistingData,
    writeln('Consulting fib_small_uc.pl...'),
    consult('fib_small_uc.pl'), nl.
    
consultFSN :-
    deleteExistingData,
    writeln('Consulting fib_small_nc.pl...'),
    consult('fib_small_nc.pl'), nl.
    
consultSS :-
    deleteExistingData,
    writeln('Consulting sor_small.pl...'),
    consult('sor_small.pl'), nl.
    
consultBLHO :-
    deleteExistingData,
    writeln('Consulting batch_large_homo.pl...'),
    consult('batch_large_homo.pl'), nl.

consultBLHE :-
    deleteExistingData,
    writeln('Consulting batch_large_hetero.pl...'),
    consult('batch_large_hetero.pl'), nl.
    
consultFLU :-
    deleteExistingData,
    writeln('Consulting fib_large_uc.pl...'),
    consult('fib_large_uc.pl'), nl.
    
consultFLN :-
    deleteExistingData,
    writeln('Consulting fib_large_nc.pl...'),
    consult('fib_large_nc.pl'), nl.
    
consultSL :-
    deleteExistingData,
    writeln('Consulting sor_large.pl...'),
    consult('sor_large.pl'), nl.
