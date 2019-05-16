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

-module(couch_jobs_notifier).

-behaviour(gen_server).


-export([
    start_link/1,
    subscribe/3,
    subscribe/4,
    unsubscribe/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include("couch_jobs.hrl").


-define(TYPE_MONITOR_HOLDOFF_DEFAULT, 1000).
-define(TYPE_MONITOR_TIMEOUT_DEFAULT, 60000).


-record(st, {
    jtx,
    type,
    monitor_pid,
    subs
}).


start_link(Type) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Type], []).


subscribe(Server, JobId, Pid) when is_pid(Pid) ->
    gen_server:call(Server, {subscribe, JobId, nil, Pid}, infinity).


subscribe(Server, JobId, Fun, Pid) when is_function(Fun, 4), is_pid(Pid) ->
    gen_server:call(Server, {subscribe, JobId, Fun, Pid}, infinity).


unsubscribe(Server, Ref) when is_reference(Ref) ->
    gen_server:call(Server, {unsubscribe, Ref}, infinity).


init([Type]) ->
    EtsOpts = [ordered_set, protected],
    JTx = couch_jobs_fdb:get_jtx(),
    St = #st{jtx = JTx, type = Type, subs = ets:new(?MODULE, EtsOpts)},
    VS = get_type_vs(St),
    Pid = couch_jobs_type_monitor:start(Type, VS, get_holdoff(), get_timeout()),
    {ok, St#st{monitor_pid = Pid}}.


terminate(_, _St) ->
    ok.


handle_call({subscribe, JobId, Fun, Pid}, _From, #st{} = St) ->
    Res = case get_job(St, JobId) of
        not_found ->
            not_found;
        {ok, _, finished} ->
            finished;
        {ok, _, JobState} ->
            Ref = erlang:monitor(process, Pid),
            ets:insert(St#st.subs, {{JobId, Ref}, {Fun, Pid, JobState}}),
            {Ref, JobState}
    end,
    {reply, Res, St};

handle_call({unsubscribe, Ref}, _From, St) ->
    true = ets:match_delete(?MODULE, {{'$1', Ref}, '_'}),
    {reply, ok, St};

% couch_jobs_type_monitor calls this
handle_call({type_updated, VS}, _From, St) ->
    ok = notify_subscribers(VS, St),
    {reply, ok, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _, _}, #st{subs = Subs} = St) ->
    true = ets:match_delete(Subs, {{'$1', Ref}, '_'}),
    {noreply, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


get_job(#st{jtx = JTx, type = Type}, JobId) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:get_job(JTx1, Type, JobId)
    end).


get_jobs(#st{jtx = JTx, type = Type}, JobIds) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        lists:map(fun(JobId) ->
            case couch_jobs_fdb:get_job(JTx1, Type, JobId) of
                {ok, _, JobState} -> {JobId, JobState};
                not_found -> {JobId, not_found}
            end
        end, JobIds)
    end).


get_type_vs(#st{jtx = JTx, type = Type}) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_job_fdb:get_activity_vs(JTx1, Type)
    end).


% "Active since" is the list of jobs that have been active (running)
% and updated at least once since the given versionstamp. These are relatively
% cheap to find as it's just a range read in the ?ACTIVITY subspace.
%
get_active_since(#st{jtx = JTx, type = Type}, VS, SubscribedJobs) ->
    AllUpdatedSet = sets:from_list(couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:get_active_since(JTx1, Type, VS)
    end)),
    SubscribedSet = sets:from_list(SubscribedJobs),
    SubscribedActiveSet = sets:intersection(AllUpdatedSet, SubscribedSet),
    sets:to_list(SubscribedActiveSet).


get_subscribers(JobId, #st{subs = Subs} = St) ->
    % Use ordered ets's fast matching of partial key prefixes here
    lists:map(fun([Ref, {Fun, Pid, JobState}]) ->
        {Ref, Fun, Pid, JobState}
    end, ets:match(Subs, {{JobId, '$1'}, '$2'})).


get_subscribed_job_ids(#st{subs = Subs}) ->
    Matches = ets:match(Subs, {{'$1', '_'}, '_'}),
    JobIds = lists:usort(lists:flatten(Matches)).


notify_subscribers(VS, #st{subs = Subs} = St) ->
    JobIds = get_subscribed_job_ids(St),
    % First gather the easy (cheap) active jobs. Then with those out of way,
    % inspect each job to get its state.
    Active = get_active_since(St, VS, JobIds),
    JobStates = [{JobId, running} || JobId <- Active],
    JobStates1 = JobStates ++ get_jobs(St, JobIds -- Active),
    lists:foreach(fun({JobId, JobState}) ->
        lists:foreach(fun
            ({Ref, Fun, Pid, JobState}) ->
                ok;
            ({Ref, Fun, Pid, _}) ->
                notify(JobId, Ref, Fun, Pid, JobState, St)
        end, get_subscribers(JobId, St)),
        case lists:member(JobState, [finished, not_found]) of
            true -> ets:match_delete(Subs, {{JobId, '_'}, '_'});
            false -> ok
        end
    end, JobStates1).


notify(JobId, Ref, Fun, _, JobState, St) when is_function(Fun, 4) ->
    try
        Fun(Ref, St#st.type, JobId, JobState)
    catch
        Tag:Err ->
            ErrMsg = "~p : callback ~p failed ~p ~p =>  ~p:~p",
            couch_log:error(ErrMsg, [?MODULE, Fun, JobId, JobState, Tag, Err])
    end;

notify(JobId, Ref, _, Pid, JobState, St) ->
    Pid ! {?COUCH_JOBS_EVENT, Ref, St#st.type, JobId, JobState}.


get_holdoff() ->
    config:get_integer("couch_jobs", "type_monitor_holdoff_msec",
        ?TYPE_MONITOR_HOLDOFF_DEFAULT).


get_timeout() ->
    config:get_integer("couch_jobs", "type_monitor_timeout_msec",
        ?TYPE_MONITOR_TIMEOUT_DEFAULT).
