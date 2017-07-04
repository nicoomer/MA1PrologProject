%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%          	  scheduler.pl      	    %
%               						%
% 	  Declarative Programming Project   %
%		Task-Parallel Scheduler 		%
%					                    %
%    		  Nicolas Omer		        %
%                                       %
%       Declarative Programming         %
%              2014-2015                %
%                                       %
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

:- [library(aggregate)].

%% Load File Data 

% The only way to completely remove a predicates clauses and properties from the system
% abolish(F, N) : abolish the predicate named F of arity N 
deleteExistingData :- 
    abolish(core, 1),
    abolish(task, 1),
    abolish(depends_on, 3),
    abolish(process_cost,3),
    abolish(channel, 4).

% Need to deleteExistingData between consults !

consultBSHO :-
    write('Consulting File : batch_small_homo.pl ...'),
    consult('batch_small_homo.pl').

consultBSHE :-
    write('Consulting File : batch_small_hetero.pl ...'),
    consult('batch_small_hetero.pl').

consultBLHO :-

    write('Consulting File : batch_large_homo.pl ...'),
    consult('batch_large_homo.pl').

consultBLHE :-

    write('Consulting File : batch_large_hetero.pl ...'),
    consult('batch_large_hetero.pl').

consultFSU :-

    write('Consulting File : fib_small_uc.pl ...'),
    consult('fib_small_uc.pl').

consultFSN :-

    write('Consulting File : fib_small_nc.pl ...'),
    consult('fib_small_nc.pl').

consultFLU :-

    write('Consulting File : fib_large_uc.pl ...'),
    consult('fib_large_uc.pl').

consultFLN :- 

    write('Consulting File : fib_large_nc.pl ...'),
    consult('fib_large_nc.pl'). 

consultSS :- 

    write('Consulting File : sor_small.pl ...'),
    consult('sor_small.pl').

consultSL :- 

    write('Consulting File : sor_large.pl ...'),
    consult('sor_large.pl').

% Where :
%   statistics(+Key, -Value)
%   Unify system statistics determined by Key with Value.
%   runtime = [ CPU time in milliseconds, CPU time in milliseconds since last... ] 
%   So only the head is relevant

main :- 
    statistics(runtime, Val),
    [StartTime | _] = Val,
    % do operation here
    deleteExistingData,
    consultBSHO,
    find_optimal(S),
    pretty_print(S),
    statistics(runtime, V),
    [EndTime | _] = V,
    Duration is (EndTime - StartTime)/1000,
    write('Program runtime = '),
    write(Duration),
    write(' sec').

%% Functional Requirements

% isSolution(+S) : 
% Checks whether an execution schedule is valid.

isSolution(solution(SolList)) :- 
    listTasks(Tasks),
    isSolution(Tasks, SolList), 
    !.

isSolution([], SolList) :- 
    listCores(CoreList), 
    findall(C, member(schedule(C, _), SolList), Cores), 
    equalList(Cores, CoreList).
    
isSolution(Tasks, SolList) :- 
    findDoableTasksInSol(Tasks, SolList, AvailableTasks),
    not(length(AvailableTasks, 0)),
    exclude(member_(AvailableTasks), Tasks, NewTasks),
    isSolution(NewTasks, SolList).

member_(List, El) :- 
    member((El, _), List).

% execution_time(+S, -ET) : 
% Where ET is the execution time of solution S.
% Execution time is defined as the latest 
% completion time of any task in the schedule.

execution_time(solution(SolList), ET) :- 
    tasks(Todo),
    execution_time(Todo, SolList, execution([]), ET), 
    !.
    
execution_time([], _, execution([]), 0).
    
execution_time([], _, execution(L), ET) :- 
    findall(Time, member((_, _, Time), L), Times),
    max_list(Times, ET).

execution_time(Todo, SolList, Exec, ET) :- 
    findDoableTasksInSol(Todo, SolList, Doable),
    addAllFinishTimes(Doable, SolList, Exec, NewExec, Todo, NewTodo),
    execution_time(NewTodo, SolList, NewExec, ET).

findDoableTasksInSol(Todo, SolList, L) :- 
    findall( (Task, Core), 
    (
        core(Core),
        member(Task, Todo), 
        not( (depends_on(Task, T, _), member(T, Todo)) ),
        firstNonExecutedCore(SolList, Todo, (Task, Core))
    )
    , L).

addExec(execution(L), (T, Core, Time), execution(Res)) :-
    append([ (T, Core, Time) ], L, Res).

findTaskExec(execution([]), _, 0).
findTaskExec(execution([ (Task, Core, Time) | _]), Task, (Task, Core, Time)).
findTaskExec(execution([_ | Tail]), Task, Res) :-
    findTaskExec(execution(Tail), Task, Res).
 
% true iff the given task is the first non executed task of its core in the given solution
firstNonExecutedCore([schedule(Core, Tasks) | _], Todo, (Task,Core)) :- 
    !,
    member(Task, Tasks),
    firstNonExecutedTaskList(Tasks, Todo, Task).
    
firstNonExecutedCore([_ | Tail], Todo, (Task,Core)) :-
    firstNonExecutedCore(Tail, Todo, (Task,Core)).

firstNonExecutedTaskList([T | _ ], _, T).
firstNonExecutedTaskList([Head | Tail], Todo, T) :- 
    not(member(Head, Todo)),
    firstNonExecutedTaskList(Tail, Todo, T).

% find_optimal(-S) :
% Finds an execution schedule S, 
% exactly minimizing execution time.

find_optimal(Solution) :- 
    initSolution(SolutionSoFar),
    listTasks(TasksTodo),
    optimal(TasksTodo, SolutionSoFar, [execution([])], Solution).

optimal([], Solutions, _, S) :-
    aggregate(min(ET, X), (member(X, Solutions), isSolution(X), execution_time(X, ET)), min(_, S)).

optimal(TasksTodo, SolutionSoFar, TaskCoreCostTriplet, Solution) :-
    availableTasks(TasksTodo, AvailableTasks),
    [CurrentTask | _] = AvailableTasks,
    delete(TasksTodo, CurrentTask, TasksLeftTodo),
    updateSolution(CurrentTask, SolutionSoFar, TaskCoreCostTriplet, NewSolution, NewTaskCoreCostTriplet),
    optimal(TasksLeftTodo, NewSolution, NewTaskCoreCostTriplet, Solution).

availableTasks(TasksTodo, AvailableTasks) :- 
    findall( CandidateTask, 
    (
        member(CandidateTask, TasksTodo),
        not((depends_on(CandidateTask, Task, _), member(Task, TasksTodo))) 
    )
    , AvailableTasks).

updateSolution(CurrentTask, SolutionSoFar, TaskCoreCostTriplet, NewSolution, NewTaskCoreCostTriplet) :- 
    listCores(Cores),
    updateSolution(Cores, CurrentTask, SolutionSoFar, TaskCoreCostTriplet, [], NewSolution, NewTaskCoreCostTriplet).
    
updateSolution([], _, _, _, NewSolution, NewSolution, _).
    
updateSolution([Core | CoresLeft], CurrentTask, SolutionSoFar, TaskCoreCostTriplet, SolutionAcc, NewSolution, NewTaskCoreCostTriplet) :-  
    addTasktoCoreLists(CurrentTask, SolutionSoFar, TaskCoreCostTriplet, NewSolutionSoFar, NewTaskCoreCostTriplet, Core),
    append(NewSolutionSoFar, SolutionAcc, NewSolutionAcc),
    updateSolution(CoresLeft, CurrentTask, SolutionSoFar, TaskCoreCostTriplet, NewSolutionAcc, NewSolution, NewTaskCoreCostTriplet).

addTasktoCoreLists(CurrentTask, [HeadSolutionSoFar | RestSolutionSoFar], TaskCoreCostTriplet, [HeadNewSolutionSoFar | RestNewSolutionSoFar], NewTaskCoreCostTriplet, Core) :-
    addTasktoCoreSol(CurrentTask, Core, HeadSolutionSoFar, HeadNewSolutionSoFar, TaskCoreCostTriplet, NewTaskCoreCostTriplet),
    addTasktoCoreLists(CurrentTask, RestSolutionSoFar, TaskCoreCostTriplet, RestNewSolutionSoFar, NewTaskCoreCostTriplet, Core).

addTasktoCoreSol(CurrentTask, Core, solution(SolutionList), solution(NewSolutionList), TaskCoreCostTriplet, NewTaskCoreCostTriplet) :-
    maplist( addTasktoCore(CurrentTask, Core), SolutionList, NewSolutionList),
    addAllFinishTimes([(CurrentTask, Core)], NewSolutionList, TaskCoreCostTriplet, NewTaskCoreCostTriplet, [CurrentTask], _).

addTasktoCore(CurrentTask, Core, schedule(Core, TaskList), Schedule) :- 
    !,
    append(TaskList, [CurrentTask], NewTaskList),
    Schedule = schedule(Core, NewTaskList).

addTasktoCore(_, _, Schedule, Schedule).

prevTaskOnCoreEndTime([schedule(Core, Tasks) | _], Exec, Task, Core, Prev) :-
    prevTaskInListEndTime(Tasks, Exec, Task, Prev).
prevTaskOnCoreEndTime([_ | Tail], Exec, Task, Core, Prev) :-
    prevTaskOnCoreEndTime(Tail, Exec, Task, Core, Prev).

prevTaskInListEndTime([Task | _], _, Task, 0).
prevTaskInListEndTime([Head | Tail], Exec, Task, Prev) :- 
    [Task | _ ] = Tail,
    !,
    findTaskExec(Exec, Head, (_, _, Prev)).
prevTaskInListEndTime([_ | Tail], Exec, Task, Prev) :- 
    prevTaskInListEndTime(Tail, Exec, Task, Prev).  

taskStartTime((Task, Core), SolList, Exec, StartTime) :-
    prevTaskOnCoreEndTime(SolList, Exec, Task, Core, PrevTime),
    findall(Time, 
    (
        depends_on(Task, X, DATA),
        findTaskExec(Exec,X, (X, XCore, XTime)),
        channel(Core, XCore, Lat, Band),
        (  Core = XCore
        -> Time is XTime
        ;  Time is XTime + Lat + DATA/Band 
        )  
    ),
    DepTimes),
    append([PrevTime], DepTimes, Times),
    max_list(Times, StartTime).
 
taskFinishTime((Task, Core), SolList, Exec, FinishTime) :-
    taskStartTime((Task, Core), SolList, Exec, StartTime),
    process_cost(Task, Core, Cost),
    FinishTime is (StartTime + Cost).

addAllFinishTimes([], _, TaskCoreCostTriplet, TaskCoreCostTriplet, Todo, Todo).
addAllFinishTimes([(Task, Core) | Tail] , SolList, TaskCoreCostTriplet, NewTaskCoreCostTriplet, Todo, NewTodo) :-
    taskFinishTime((Task, Core), SolList, TaskCoreCostTriplet, FinishTime),
    addExec(TaskCoreCostTriplet, (Task, Core, FinishTime), ChangedTaskCoreCostTriplet),
    delete(Todo, Task, NTodo),
    addAllFinishTimes(Tail, SolList, ChangedTaskCoreCostTriplet, NewTaskCoreCostTriplet, NTodo, NewTodo).




%% Helper Functions

% listTasks(-L) :
% Lists all input tasks
listTasks(List) :- findall(Task, task(Task), List).

% listCores(-L) :
% Lists all input cores
listCores(List) :- findall(Core, core(Core), List).

% Where maplist(:Goal, ?List1, ?List2)
% True if Goal can successfully be applied on pairs of elements from two lists.

initSolution(EmptySolution) :-
    listCores(Cores),
    EmptySolution = solution(SchedulesList),
    maplist(initSchedule, Cores, SchedulesList).

initSchedule(Core, Schedule) :-
    Schedule = schedule(Core, []).

% equalList(+List1, +List2)
% checks if List1 and List2 have the same elements

equalList(List1, List2) :-
    isSubset(List1, List2),
    isSubset(List2, List1).

% isSubset(+List1, +List2)
% Check if List1 is a subset of List2
% Where select(?Elem, ?List1, ?List2)
% Is true when List1, with Elem removed, results in List2.

isSubset([], _).
isSubset([Head | Tail], List1) :-
    select(Head, List1, List2),
    !,
    isSubset(Tail, List2).

% Print Functions

printList([]) :-
	write('[]').

printList(_, [Last]) :-
	!, % The last element is met, don't backtrack anymore
	write(Last),
	write('.').

printList(Separator, [First | Rest]) :- 
	write(First),
	write(Separator),
	printList(Separator, Rest).

printScheduling([schedule(Core, Tasks) | Rest ]) :- 
    write('Schedule found for core '),
    write(Core),
    write(' : '),
    printList(Tasks),
    nl, 
    printScheduling(Rest). 

% pretty_print(+S) : 
% Outputs an execution schedule nicely, 
% in human readable format.

pretty_print(solution(SchedulesList)) :-
    nl,
    write('Solution found :'),
    printScheduling(SchedulesList),
    printTime(solution(SchedulesList)).

printTime(Solution) :-
    execution_time(Solution, ET),
    write('in '), 
    write(ET),
    write(' seconds.').
