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

-module(couch_jobs_activity_monitor).

-behaviour(gen_server).


-export([
    start_link/1
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-record(st, {
    jtx,
    type,
    tref,
    timeout = 0,
    vs = null
}).


-define(MAX_JITTER_DEFAULT, 15000).


start_link(Type) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Type], []).


%% gen_server callbacks

init([Type]) ->
    St = #st{jtx = couch_jobs_fdb:get_jtx(), type = Type},
    {ok, schedule_check(St)}.


terminate(_, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(check_activity, St) ->
    St1 = check_activity(St),
    St2 = schedule_check(St1),
    {noreply, St2};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


check_activity(#st{vs = null} = St) ->
    St#st{vs = get_vs(St)};


check_activity(#st{} = St) ->
    NewVS = get_vs(St),
    St1 = case get_inactive_since(St) of
        [] -> St;
        [_ | _] -> re_enqueue_inactive(St)
    end,
    St1#st{vs = NewVS}.


get_timeout_msec(JTx, Type) ->
    TimeoutSec = couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:get_activity_timeout(JTx1, Type)
    end),
    timer:seconds(TimeoutSec).


schedule_check(#st{jtx = JTx, type = Type, timeout = OldTimeout} = St) ->
    % Reset versionstamp if timeout changed.
    St1 = case get_timeout_msec(JTx, Type) of
        OldTimeout -> St;
        NewTimeout -> St#st{vs = null, timeout = NewTimeout}
    end,
    #st{timeout = Timeout} = St1,
    MaxJitter = max(Timeout, get_max_jitter_msec()),
    Wait = Timeout + rand:uniform(min(1, MaxJitter)),
    St1#st{tref = erlang:set_after(Wait, self(), check_activity)}.


get_vs(#st{jtx = JTx, type = Type}) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:get_activity_vs(JTx1, Type)
    end).


re_enqueue_inactive(#st{jtx = JTx, type = Type, vs = VS}) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_jobs_fdb:re_enqueue_inactive(JTx1, Type, VS)
    end).


get_inactive_since(#st{jtx = JTx, type = Type, vs = VS}) ->
    couch_jobs_fdb:tx(JTx, fun(JTx1) ->
        couch_job_fdb:get_inactive_since(JTx1, Type, VS)
    end).


get_max_jitter_msec()->
    config:get_integer("couch_jobs", "activity_monitor_max_jitter_msec",
        ?MAX_JITTER_DEFAULT).
