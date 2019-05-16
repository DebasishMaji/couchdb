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

-module(couch_jobs_pending).


-export([
    enqueue/4,
    dequeue/3,
    remove/4
]).


-include("couch_jobs.hrl").


% Make this configurable or auto-adjustable based on retries
%
-define(RANGE_LIMIT, 256).


% Data model
%
% (?JOBS, ?PENDING, Type, Priority, JobId) = ""


% Public API

% Enqueue a job into the pending queue. Priority determines the place in the
% queue.
%
enqueue(#{jtx := true} = JTx, Type, Priority, JobId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    case couch_jobs_fdb:has_versionstamp(Priority) of
        true ->
            Key = erlfdb_tuple:pack_vs({?PENDING, Type, Priority, JobId}, Jobs),
            erlfdb:set_versionstamped_key(Tx, Key, null);
        false ->
            Key = erlfdb_tuple:pack({?PENDING, Type, Priority, JobId}, Jobs),
            erlfdb:set(Tx, Key, null)
    end.


% Dequeue a job from the front of the queue.
%
% If MaxPriority is specified, any job between the front of the queue and
% up until MaxPriority is considered. Workers may randomly pick jobs in that
% range. That can be used to avoid contention at the expense of strict dequeue
% ordering. For instance if priorities are 0-high, 1-normal and 2-low, and we wish
% to process normal and urgent jobs, then MaxPriority=1-normal would
% accomplish that.
%
dequeue(#{jtx := true} = JTx, Type, undefined) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Prefix = erlfdb_tuple:pack({?PENDING, Type}, Jobs),
    case get_front_priority(Tx, Prefix) of
        not_found ->
            not_found;
        Priority ->
            {Start, End} = erlang_tuple:range(Priority, Prefix),
            case clear_random_key_from_range(Tx, Start, End) of
                not_found ->
                    not_found;
                <<_/binary>> = PendingKey ->
                    {_, JobId} = erlfdb_tuple:unpack(PendingKey, Prefix),
                    JobId
            end
    end;

% If MaxPriority is not specified, only jobs with the same priority as the item
% at the front of the queue are considered. Two extremes are useful to
% consider:
%
%  * Priority is just one static value (say null, or "normal"). In that case,
% the queue is effectively a bag of tasks that can be grabbed in any order,
% which should minimize contention.
%
%  * Each job has a unique priority value, for example a versionstamp. In that
%  case, the queue has strict FIFO behavior, but there will be more contention
%  at the front of the queue.
%
dequeue(#{jtx := true} = JTx, Type, MaxPriority) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Prefix = erlfdb_tuple:pack({?PENDING, Type}, Jobs),
    StartKeySel = erlfdb_key:first_greater_than(Prefix),
    End = erlfdb_tuple:pack({?PENDING, Type, MaxPriority, <<16#FF>>}, Prefix),
    EndKeySel = erlfdb_key:last_less_than(End),
    case clear_random_key_from_range(Tx, StartKeySel, EndKeySel) of
        not_found ->
            not_found;
        <<_/binary>> = PendingKey ->
            {_, JobId} = erlfdb_tuple:unpack(PendingKey, Prefix),
            JobId
   end.


% Remove a job from the pending queue. This is used, for example, when a job is
% canceled while it was waiting in the pending queue.
%
remove(#{jtx := true} = JTx, Type, Priority, JobId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Key = erlfdb_tuple:pack({?PENDING, Type, Priority, JobId}, Jobs),
    erlfdb:clear(Tx, Key).


% Private functions

% The priority of the item at the front. If there are multiple
% items with the same priority, workers can randomly pick between them to
% avoid contention.
%
get_front_priority(Tx, Prefix) ->
    Opts = [{limit, 1}, {snapshot, true}],
    case erlfdb:wait(erfldb:get_range_startswith(Tx, Prefix, Opts)) of
        [] ->
            not_found;
        [{FrontKey, _}] ->
            {Priority, _} = erlfdb_tuple:unpack(FrontKey, Prefix),
            Priority
    end.


% Pick a random key from the range snapshot. Then radomly pick a key to
% clear. Before clearing, ensure there is a read conflict on the key in
% in case other workers have picked the same key.
%
clear_random_key_from_range(Tx, Start, End) ->
    Opts = [
        {limit, ?RANGE_LIMIT},
        {snapshot, true}
    ],
    case erlfdb:wait(erlfdb:get_range(Tx, Start, End, Opts)) of
        [] ->
            not_found;
        [{_, _} | _] = KVs ->
            Index = rand:uniform(length(KVs)),
            {Key, _} = lists:nth(Index),
            erlfdb:add_read_conflict_key(Tx, Key),
            erlfdb:clear(Tx, Key),
            Key
    end.
