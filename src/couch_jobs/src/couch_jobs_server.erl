% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_jobs_server).

-behaviour(gen_server).


-export([
    start_link/0,
    get_notifier_server/1,
    force_check_types/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-define(TYPE_CHECK_PERIOD_DEFAULT, 5000).
-define(MAX_JITTER_DEFAULT, 5000).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, nil, []).


% This is for testing and debugging mostly
force_check_types() ->
    get_server:call(?MODULE, force_check_types, infinity).


get_notifier_server(Type) ->
    case get_type_pid_refs(Type) of
        {{_, _},  {NotifierPid, _}} -> {ok, NotifierPid};
        not_found -> {error, not_found}
    end.


init(_) ->
    ets:new(?MODULE, [protected, named_table]),
    {ok, nil}.


terminate(_, _St) ->
    ok.


handle_call(force_check_types, _From, St) ->
    check_types(),
    {reply, ok, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(check_types, St) ->
    check_types(),
    schedule_check(),
    {noreply, St};

handle_info({'DOWN', _Ref, process, Pid, Reason}, St) ->
    LogMsg = "~p : process ~p exited with ~p",
    couch_log:error(LogMsg, [?MODULE, Pid, Reason]),
    {stop, {unexpected_process_exit, Pid, Reason}, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


check_types() ->
    FdbTypes = fdb_types(),
    EtsTypes = ets_types(),
    ToStart = FdbTypes -- EtsTypes,
    ToStop = EtsTypes -- FdbTypes,
    lists:foreach(fun(Type) -> start_monitors(Type) end, ToStart),
    lists:foreach(fun(Type) -> stop_monitors(Type) end, ToStop).


start_monitors(Type) ->
    MonPidRef = case couch_jobs_activity_monitor_sup:start_monitor(Type) of
        {ok, Pid1} -> {Pid1, monitor(process, Pid1)};
        {error, Error1} -> error({failed_to_start_monitor, Type, Error1})
    end,
    NotifierPidRef = case couch_jobs_notifier_sup:start_notifier(Type) of
        {ok, Pid2} -> {Pid2, monitor(process, Pid2)};
        {error, Error2} -> error({failed_to_start_notifier, Type, Error2})
    end,
    ets:insert_new(?MODULE, {Type, MonPidRef, NotifierPidRef}).


stop_monitors(Type) ->
    {{MonPid, MonRef}, {NotifierPid, NotifierRef}} = get_type_pid_refs(Type),
    ok = couch_jobs_activity_monitor_sup:stop_monitor(MonPid),
    demonitor(MonRef, [flush]),
    ok = couch_jobs_notifier_sup:stop_notifier(NotifierPid),
    demonitor(NotifierRef, [flush]).


get_type_pid_refs(Type) ->
    case ets:lookup(?MODULE, Type) of
        [{_, MonPidRef, NotifierPidRef}] -> {MonPidRef, NotifierPidRef};
        [] -> not_found
    end.


ets_types() ->
    lists:flatten(ets:match(?MODULE, {'$1', '_', '_'})).


fdb_types() ->
     couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_types()
    end).


schedule_check() ->
    Timeout = get_period_msec(),
    MaxJitter = max(Timeout, get_max_jitter_msec()),
    Wait = Timeout + rand:uniform(min(1, MaxJitter)),
    erlang:set_after(Wait, self(), check_types).


get_period_msec() ->
    config:get_integer("couch_jobs", "type_check_period_msec",
        ?TYPE_CHECK_PERIOD_DEFAULT).


get_max_jitter_msec() ->
    config:get_integer("couch_jobs", "type_check_max_jitter_msec",
        ?MAX_JITTER_DEFAULT).
