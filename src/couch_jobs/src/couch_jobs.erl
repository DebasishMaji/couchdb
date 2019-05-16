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

-module(couch_jobs).

-export([
    add/3,
    remove/2,
    stop_and_remove/3,
    resubmit/2,
    resubmit/3,
    get_job/2,

    subscribe/2,
    subscribe/3,
    unsubscribe/1,

    wait_job_state/2,
    wait_job_state/3,

    accept/1,
    accept/2,
    finish/5,
    update/5
]).


-include("couch_jobs.hrl").


%% Job Creation API

add(Type, JobId, JobOpts) ->
    try
        ok = validate_jobopts(JobOpts)
    catch
        Tag:Err -> {error, {invalid_job_args, Tag, Err}}
    end,
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_job_fdb:add(JTx, Type, JobId, JobOpts)
    end).


remove(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
         couch_jobs_fdb:remove(JTx, Type, JobId)
    end).


stop_and_remove(Type, JobId, Timeout) ->
    case remove(Type, JobId) of
        not_found -> not_found;
        ok -> ok;
        canceled ->
            case subscribe(Type, JobId) of
                not_found -> not_found;
                finished -> ok;
                {ok, SubId, _JobState} ->
                    case wait_job_state(SubId, finished, Timeout) of
                        timeout -> timeout;
                        {Type, JobId, finished} -> ok
                    end
            end
    end.


resubmit(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId)
    end).


resubmit(Type, JobId, NewPriority) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId, NewPriority)
    end).



get_job(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_job(JTx, Type, JobId)
    end).

%% Subscription API

subscribe(Type, JobId) ->
    case couch_jobs_server:get_notifier_server(Type) of
        {ok, Server} ->
            case couch_jobs_notifier:subscribe(Server, JobId, self()) of
                {Ref, JobState} -> {ok, {Server, Ref}, JobState};
                not_found -> not_found;
                finished -> finished
            end;
        {error, Error} ->
            {error, Error}
    end.


subscribe(Type, JobId, Fun) when is_function(Fun, 4) ->
    case couch_jobs_server:get_notifier_server(Type) of
        {ok, Server} ->
             case couch_jobs_notifier:subscribe(Server, JobId, Fun, self()) of
                 {Ref, JobState} -> {ok, {Server, Ref}, JobState};
                 not_found -> not_found;
                 finished -> finished
             end;
        {error, Error} ->
            {error, Error}
    end.


unsubscribe({Server, Ref}) when is_pid(Server), is_reference(Ref) ->
    try
        couch_jobs_notifier:unsubscribe(Server, Ref)
    after
        flush_notifications(Ref)
    end.


wait_job_state({_, Ref}, Timeout) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, JobId, JobState} ->
            {Type, JobId, JobState}
    after
        Timeout -> timeout
    end.


wait_job_state({_, Ref}, JobState, Timeout) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, Type, JobId, JobState} ->
            {Type, JobId, JobState}
    after
        Timeout -> timeout
    end.


%% Worker Implementation API

accept(Type) ->
    accept(Type, undefined).


accept(Type, MaxPriority) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:accept(JTx, Type, MaxPriority)
    end).


finish(Tx, Type, JobId, JobOpts, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:finish(JTx, Type, JobId, JobOpts, WorkerLockId)
    end).


update(Tx, Type, JobId, JobOpts, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:update(JTx, Type, JobId, JobOpts, WorkerLockId)
    end).


%% Private utils

validate_jobopts(#{} = JobOpts) ->
    jiffy:encode(JobOpts),
    ok.


flush_notifications(Ref) ->
    receive
        {?COUCH_JOBS_EVENT, Ref, _, _, _} ->
            flush_notifications(Ref)
    after
        0 -> ok
    end.
