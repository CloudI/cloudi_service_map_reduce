%-*-Mode:erlang;coding:utf-8;tab-width:4;c-basic-offset:4;indent-tabs-mode:()-*-
% ex: set ft=erlang fenc=utf-8 sts=4 ts=4 sw=4 et nomod:
%%%
%%%------------------------------------------------------------------------
%%% @doc
%%% ==CloudI Task Size Calculation==
%%% Determine the task size that can be processed in a node's Erlang process
%%% while utilizing a single timeout value for all requests.
%%% The single timeout value is based on the target time in hours.
%%% The goal is to have all requests taking the same target time while
%%% the task size varies based on the node's execution speed.
%%% @end
%%%
%%% MIT License
%%%
%%% Copyright (c) 2009-2024 Michael Truog <mjtruog at protonmail dot com>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a
%%% copy of this software and associated documentation files (the "Software"),
%%% to deal in the Software without restriction, including without limitation
%%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%%% and/or sell copies of the Software, and to permit persons to whom the
%%% Software is furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in
%%% all copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
%%% DEALINGS IN THE SOFTWARE.
%%%
%%% @author Michael Truog <mjtruog at protonmail dot com>
%%% @copyright 2009-2024 Michael Truog
%%% @version 2.0.8 {@date} {@time}
%%%------------------------------------------------------------------------

-module(cloudi_task_size).
-author('mjtruog at protonmail dot com').

%% external interface
-export([get/2,
         new/7,
         put/4,
         reduce/2,
         reduce/3,
         task_count/1]).

-include_lib("cloudi_core/include/cloudi_logger.hrl").

-record(node,
    {
        task_size :: number()
    }).

-record(cloudi_task_size,
    {
        task_count :: pos_integer(), % count of concurrent tasks
        task_size_initial :: pos_integer(), % count to control task size
        task_size_min :: pos_integer(),
        task_size_max :: pos_integer(),
        target_time :: float(), % in hours
        target_time_min :: float(), % in hours
        target_time_max :: float(), % in hours
        target_time_incr = 0 :: non_neg_integer(),
        target_time_decr = 0 :: non_neg_integer(),
        lookup = #{} :: #{node() := #node{}}
    }).

-type state() :: #cloudi_task_size{}.
-export_type([state/0]).

-define(TARGET_TIME_ADJUST, 4). % number of consecutive incr/decr to
                                % cause a target time adjustment
-define(TARGET_TIME_ADJUST_FACTOR, 2.0).
-define(TARGET_TIME_USAGE_FACTOR, 2.0). % defines task size tolerance

%%%------------------------------------------------------------------------
%%% External interface functions
%%%------------------------------------------------------------------------

%%-------------------------------------------------------------------------
%% @doc
%% ===Get the task size information.===
%% @end
%%-------------------------------------------------------------------------

-spec get(Pid :: pid(),
          State :: state()) ->
    {TaskSize :: pos_integer(),
     Timeout :: cloudi_service:timeout_value_milliseconds()}.

get(Pid,
    #cloudi_task_size{task_size_initial = TaskSizeInitial,
                      target_time = TargetTime,
                      lookup = Lookup})
    when is_pid(Pid) ->
    Timeout = erlang:round(?TARGET_TIME_USAGE_FACTOR *
                           TargetTime * 3600000.0),
    case maps:find(node(Pid), Lookup) of
        {ok, #node{task_size = TaskSize}} ->
            TaskSizeInteger = if
                is_float(TaskSize) ->
                    floor(TaskSize);
                is_integer(TaskSize) ->
                    TaskSize
            end,
            {TaskSizeInteger, Timeout};
        error ->
            {TaskSizeInitial, Timeout}
    end.

%%-------------------------------------------------------------------------
%% @doc
%% ===Get a new task size lookup.===
%% @end
%%-------------------------------------------------------------------------

-spec new(TaskCount :: pos_integer(),
          TaskSizeInitial :: pos_integer(),
          TaskSizeMin :: pos_integer(),
          TaskSizeMax :: pos_integer(),
          TargetTimeInitial :: float(),
          TargetTimeMin :: float(),
          TargetTimeMax :: float()) ->
    state().

new(TaskCount,
    TaskSizeInitial, TaskSizeMin, TaskSizeMax,
    TargetTimeInitial, TargetTimeMin, TargetTimeMax)
    when is_integer(TaskCount), TaskCount > 0,
         is_integer(TaskSizeInitial),
         is_integer(TaskSizeMin), TaskSizeMin > 0, is_integer(TaskSizeMax),
         TaskSizeInitial >= TaskSizeMin, TaskSizeInitial =< TaskSizeMax,
         is_float(TargetTimeInitial), TargetTimeInitial > 0.0,
         is_float(TargetTimeMin), is_float(TargetTimeMax),
         TargetTimeInitial >= TargetTimeMin,
         TargetTimeInitial =< TargetTimeMax ->
    #cloudi_task_size{task_count = TaskCount,
                      task_size_initial = TaskSizeInitial,
                      task_size_min = TaskSizeMin,
                      task_size_max = TaskSizeMax,
                      target_time = TargetTimeInitial,
                      target_time_min = TargetTimeMin,
                      target_time_max = TargetTimeMax}.

%%-------------------------------------------------------------------------
%% @doc
%% ===Store task size information.===
%% ElapsedTime is in hours.
%% @end
%%-------------------------------------------------------------------------

-spec put(Pid :: pid(),
          TaskSize :: pos_integer(),
          ElapsedTime :: float(),
          State :: state()) ->
    state().

put(Pid, TaskSize, ElapsedTime,
    #cloudi_task_size{task_count = TaskCount,
                      task_size_initial = TaskSizeInitial,
                      task_size_min = TaskSizeMin,
                      task_size_max = TaskSizeMax,
                      target_time = TargetTime,
                      target_time_incr = TargetTimeIncr,
                      target_time_decr = TargetTimeDecr,
                      target_time_min = TargetTimeMin,
                      target_time_max = TargetTimeMax,
                      lookup = Lookup} = State)
    when is_pid(Pid), is_integer(TaskSize), is_float(ElapsedTime) ->
    Node = node(Pid),
    #node{task_size = TaskSizeOld} = NodeState = case maps:find(Node, Lookup) of
        {ok, LookupValue} ->
            LookupValue;
        error ->
            #node{task_size = TaskSizeInitial}
    end,
    TaskSizeSmoothed = task_size_smoothed(TaskSize, TaskSizeOld,
                                          TargetTime, ElapsedTime, TaskCount),
    {TaskSizeClamp,
     TaskSizeNew} = task_size_clamp(TaskSizeSmoothed, TaskSizeMin, TaskSizeMax),
    {TargetTimeIncrNew,
     TargetTimeDecrNew,
     TargetTimeNew} = if
        TaskSizeClamp =:= minimum ->
            {TargetTimeIncrNext,
             TargetTimeNext} = target_time_incr(TargetTimeIncr + 1,
                                                TargetTime, TargetTimeMax),
            {TargetTimeIncrNext, 0, TargetTimeNext};
        TaskSizeClamp =:= maximum ->
            {TargetTimeDecrNext,
             TargetTimeNext} = target_time_decr(TargetTimeDecr + 1,
                                                TargetTime, TargetTimeMin),
            {0, TargetTimeDecrNext, TargetTimeNext};
        TaskSizeClamp =:= clamped ->
            {0, 0, TargetTime}
    end,
    LookupNew = maps:put(Node,
                         NodeState#node{task_size = TaskSizeNew},
                         Lookup),
    State#cloudi_task_size{target_time = TargetTimeNew,
                           target_time_incr = TargetTimeIncrNew,
                           target_time_decr = TargetTimeDecrNew,
                           lookup = LookupNew}.

%%-------------------------------------------------------------------------
%% @doc
%% ===Reduce the task size by 10% after a timeout.===
%% @end
%%-------------------------------------------------------------------------

-spec reduce(Pid :: pid(),
             State :: state()) ->
    state().

reduce(Pid, State) ->
    reduce(Pid, 0.9, State).

%%-------------------------------------------------------------------------
%% @doc
%% ===Reduce the task size after a timeout.===
%% @end
%%-------------------------------------------------------------------------

-spec reduce(Pid :: pid(),
             Multiplier :: float(),
             State :: state()) ->
    state().

reduce(Pid, Multiplier,
       #cloudi_task_size{task_size_min = TaskSizeMin,
                         task_size_max = TaskSizeMax,
                         lookup = Lookup} = State)
    when is_pid(Pid), is_float(Multiplier),
         Multiplier > 0.0, Multiplier =< 1.0 ->
    Node = node(Pid),
    case maps:find(Node, Lookup) of
        {ok, #node{task_size = TaskSize} = NodeState} ->
            {_, TaskSizeNew} = task_size_clamp(TaskSize * Multiplier,
                                               TaskSizeMin, TaskSizeMax),
            NodeStateNew = NodeState#node{task_size = TaskSizeNew},
            State#cloudi_task_size{lookup = maps:put(Node, NodeStateNew,
                                                     Lookup)};
        error ->
            State
    end.

%%-------------------------------------------------------------------------
%% @doc
%% ===Return the task count used for initialization.===
%% @end
%%-------------------------------------------------------------------------

-spec task_count(state()) ->
    pos_integer().

task_count(#cloudi_task_size{task_count = TaskCount}) ->
    TaskCount.

%%%------------------------------------------------------------------------
%%% Private functions
%%%------------------------------------------------------------------------

task_size_clamp(TaskSize, TaskSizeMin, TaskSizeMax)
    when is_float(TaskSize) ->
    TaskSizeInteger = floor(TaskSize),
    if
        TaskSizeInteger < TaskSizeMin ->
            {minimum, TaskSizeMin};
        TaskSizeInteger > TaskSizeMax ->
            {maximum, TaskSizeMax};
        true ->
            {clamped, TaskSize}
    end.

task_size_smoothed(TaskSizeCurrent, TaskSizeOld,
                   TargetTimeTotal, ElapsedTime, TaskCount)
    when is_integer(TaskSizeCurrent), is_number(TaskSizeOld),
         is_float(TargetTimeTotal), is_float(ElapsedTime) ->
    TargetTime = TargetTimeTotal / ?TARGET_TIME_USAGE_FACTOR,
    TaskSizeNext = TaskSizeCurrent * (TargetTime / ElapsedTime),
    Difference = erlang:abs((TargetTime - ElapsedTime) / ElapsedTime),
    % determine the truncated moving average period based on the
    % percentage difference between the elapsed time and the target time
    % (empirically found solution that is relatively stable
    %  and provides slow convergance, until a better solution is found)
    %product(L) when is_list(L) ->
    %    lists:foldl(fun(X, Y) -> X * Y end, 1, L).
    %ceil(X) ->
    %    T = erlang:trunc(X),
    %    if
    %        X > T ->
    %            T + 1;
    %        true ->
    %            T
    %    end.
    %floor(X) ->
    %   T = erlang:trunc(X),
    %   if
    %       X < T ->
    %           T - 1;
    %       true ->
    %           T
    %   end.
    %SmoothingFactor = if
    %    Difference =< 1.0 ->
    %        product(lists:seq(
    %            floor(math:log(Difference) / math:log(3.0)) * -2 + 1,
    %        1, -2)) * 2 * erlang:float(TaskCount);
    %    true ->
    %        product(lists:seq(
    %            ceil(math:log(Difference) / math:log(50.0)) * 2 + 1,
    %        1, -2)) * 2 * erlang:float(TaskCount)
    %end,
    % smoothing method as separate sequences
    SmoothingFactor = TaskCount * (if
        Difference =< 0.0625 ->
            1890.0;
        Difference =< 0.125 ->
            210.0;
        Difference =< 0.25 ->
            30.0;
        Difference =< 0.5 ->
            6.0;
        Difference =< 1.0 ->
            2.0;
        Difference =< 50.0 ->                   % = 1.0 * 50
            6.0;                                % = 2.0 * 3
        Difference =< 2500.0 ->                 % = 50.0 * 50
            30.0;                               % = 6.0 * 5
        Difference =< 125000.0 ->               % = 2500.0 * 50
            210.0;                              % = 30.0 * 7
        Difference =< 6250000.0 ->              % = 125000.0 * 50
            1890.0;                             % = 210.0 * 9
        true ->
            20790.0                             % = 1890.0 * 11
    end),
    % perform truncated moving average
    Change = (TaskSizeNext - TaskSizeOld) / SmoothingFactor,
    TaskSizeOld + Change.

target_time_incr(?TARGET_TIME_ADJUST, TargetTime, TargetTimeMax) ->
    TargetTimeNew = erlang:min(TargetTime * ?TARGET_TIME_ADJUST_FACTOR,
                               TargetTimeMax),
    if
        TargetTimeNew == TargetTimeMax ->
            ?LOG_TRACE("target time limited by maximum (~p hours)",
                       [TargetTimeMax]);
        true ->
            ?LOG_TRACE("target time increased to ~p hours", [TargetTimeNew])
    end,
    {0, TargetTimeNew};
target_time_incr(TargetTimeIncr, TargetTime, _) ->
    {TargetTimeIncr, TargetTime}.

target_time_decr(?TARGET_TIME_ADJUST, TargetTime, TargetTimeMin) ->
    TargetTimeNew = erlang:max(TargetTime / ?TARGET_TIME_ADJUST_FACTOR,
                               TargetTimeMin),
    if
        TargetTimeNew == TargetTimeMin ->
            ?LOG_TRACE("target time limited by minimum (~p hours)",
                       [TargetTimeMin]);
        true ->
            ?LOG_TRACE("target time decreased to ~p hours", [TargetTimeNew])
    end,
    {0, TargetTimeNew};
target_time_decr(TargetTimeDecr, TargetTime, _) ->
    {TargetTimeDecr, TargetTime}.

