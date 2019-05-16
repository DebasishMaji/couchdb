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

-module(couch_jobs_fdb).


-export([
    add/4,
    remove/3,
    resubmit/4,
    get_job/3,

    accept/2,
    accept/3,
    finish/5,
    update/5,

    set_activity_timeout/3,
    get_activity_timeout/2,

    get_types/1,

    get_activity_vs/2,
    get_activity_vs_and_watch/2,
    get_active_since/3,
    re_enqueue_inactive/3,

    clear_type/2,

    init_cache/0,

    get_jtx/0,
    get_jtx/1,

    tx/2
]).


-include("couch_jobs.hrl").


% Data model
%
% (?JOBS, ?DATA, Type, JobId) = (Sequence, WorkerLockId, Priority, JobOpts)
% (?JOBS, ?PENDING, Type, Priority, JobId) = ""
% (?JOBS, ?WATCHES, Type) = Sequence
% (?JOBS, ?ACTIVITY_TIMEOUT, Type) = ActivityTimeout
% (?JOBS, ?ACTIVITY, Type, Sequence) = JobId

% Job creation API

add(#{jtx := true} = JTx0, Type, JobId, JobOpts) ->
    #{tx := Tx, jobs_path := Jobs} = JTx = get_jtx(JTx0),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> ->
            {error, duplicate_job};
        not_found ->
            maybe_enqueue(JTx, Type, JobId, JobOpts)
    end.


remove(#{jtx := true} = JTx0, Type, JobId) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx = get_jtx(JTx0),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {_, WorkerLockId, _, _} = Job when WorkerLockId =/= null ->
            ok = cancel(JTx, Key, Job),
            canceled;
        {_, _, null, _,  _} ->
            erlfdb:clear(Tx, Key),
            ok;
        {_, _, Priority, _, _} ->
            couch_jobs_pending:remove(JTx, Type, Priority, JobId),
            erlfdb:clear(Tx, Key),
            ok;
        not_found ->
            not_found
    end.


resubmit(#{jtx := true} = JTx, Type, JobId, NewPriority) ->
    #{tx := Tx, jobs_path :=  Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {_, _, _, #{?OPT_RESUBMIT := true}} ->
            ok;
        {Seq, WorkerLockId, Priority, #{} = JobOpts} ->
            JobOpts1 = JobOpts#{?OPT_RESUBMIT => true},
            OldPriority = maps:get(?OPT_PRIORITY, JobOpts1, undefined),
            JobOpts2 = case NewPriority =/= OldPriority of
                true -> JobOpts1#{?OPT_PRIORITY => NewPriority};
                false -> JobOpts1
            end,
            JobOptsEnc = jiffy:encode(JobOpts2),
            % Don't update priority value from the tuple as that points to entry
            % in pending queue. Only update the one from JobOpts and it will be
            % used when the job is re-enqueued later
            Val = erlfdb_tuple:pack({Seq, WorkerLockId, Priority, JobOptsEnc}),
            erlfdb:set(Tx, Key, Val),
            ok;
        not_found ->
            not_found
    end.


get_job(#{jtx := true} = JTx, Type, JobId) ->
    #{tx := Tx, jobs_path :=  Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {_, WorkerLockId, Priority, JobOpts} ->
            {ok, JobOpts, job_state(WorkerLockId, Priority)};
        not_found ->
            not_found
    end.


% Worker public API

accept(#{jtx := true} = JTx, Type) ->
    accept(JTx, Type, undefined).


accept(#{jtx := true} = JTx0, Type, MaxPriority) ->
    #{jtx := true} = JTx = get_jtx(JTx0),
    case couch_jobs_pending:dequeue(JTx, Type, MaxPriority) of
        not_found ->
            not_found;
        <<_/binary>> = JobId ->
            WorkerLockId = fabric2_util:uuid(),
            update_lock(JTx, Type, JobId, WorkerLockId),
            update_activity(JTx, Type, JobId),
            update_watch(JTx, Type),
            {ok, JobId, WorkerLockId}
    end.


finish(#{jtx := true} = JTx0, Type, JobId, JobOpts, WorkerLockId) ->
    #{tx := Tx, jobs_path := Jobs} = JTx = get_jtx(JTx0),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job_and_status(Tx, Key, WorkerLockId) of
        {Status, {Seq, _, _, JobOptsCur}} when
                Status =:= ok orelse Status =:= canceled ->
            % If the job was canceled, allow updating its data one last time
            clear_activity(JTx, Type, Seq),
            MergedOpts = maps:merge(JobOptsCur, JobOpts),
            maybe_enqueue(JTx, Type, JobId, MergedOpts),
            update_watch(JTx, Type),
            ok;
        {worker_conflict, _} ->
            worker_conflict
    end.


update(#{jtx := true} = JTx, Type, JobId, JobOpts, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job_and_status(Tx, Key, WorkerLockId) of
        {ok, {null, WorkerLockId, null, JobOptsCur}} ->
            update_activity(JTx, Type, JobId),
            JobOpts1 = maps:merge(JobOptsCur, JobOpts),
            JobOptsEnc = jiffy:encode(JobOpts1),
            Val = erlfdb_tuple:pack({null, WorkerLockId, null, JobOptsEnc}),
            erlfdb:set(Tx, Key, Val),
            update_watch(JTx, Type);
        {ok, InvalidState} ->
            error({couch_job_invalid_updata_state, InvalidState});
        {Status, _} when Status =/= ok ->
            Status
    end.


% Type and activity monitoring API

set_activity_timeout(#{jtx := true} = JTx, Type, Timeout) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT, Type}, Jobs),
    Val = erlfdb_tuple:pack(Timeout),
    erlfdb:set(Tx, Key, Val).


get_activity_timeout(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT, Type}, Jobs),
    Val = erlfdb:wait(erlfdb:get(Tx, Key)),
    erlfdb_tuple:unpack(Val).


get_types(#{jtx := true} = JTx) ->
    #{tx := Tx, type := Type, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?ACTIVITY_TIMEOUT}, Jobs),
    Opts = [{streaming_mode, want_all}],
    Future = erlfdb:get_range_startswith(Tx, Prefix, Opts),
    lists:map(fun(K, _V) ->
        Type = erfdb_tuple:unpack(K, Prefix)
    end, erlfdb:wait(Future)).


get_activity_vs(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?WATCHES, Type}, Jobs),
    erlfdb:wait(erlfdb:get(Tx, Key)).


get_activity_vs_and_watch(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Key = erlfdb_tuple:pack({?WATCHES, Type}, Jobs),
    {Val, WatchFuture} = erlfdb:get_and_watch(Tx, Key),
    {Val, WatchFuture}.


get_active_since(#{jtx := true} = JTx, Type, Versionstamp) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tupe:pack({?ACTIVITY}, Jobs),
    StartKey = erlfdb_tuple:pack({Type, Versionstamp}, Prefix),
    StartKeySel = erlfdb_key:first_greater_than(StartKey),
    {_, EndKey} = erlfdb_tuple:range({Type}, Prefix),
    Opts = [{streaming_mode, want_all}],
    Future = erlfdb:get_range(Tx, StartKeySel, EndKey, Opts),
    lists:map(fun({_K, JobId}) -> JobId end, erlfdb:wait(Future)).


get_inactive_since(#{jtx := true} = JTx, Type, Versionstamp) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    Prefix = erlfdb_tuple:pack({?ACTIVITY}, Jobs),
    {StartKey, _} = erlfdb_tuple:range({Type}, Prefix),
    EndKey = erlfdb_tuple:pack({Type, Versionstamp}, Prefix),
    EndKeySel = erlfdb_key:last_less_or_equal(EndKey),
    Opts = [{streaming_mode, want_all}],
    Future = erlfdb:get_range(Tx, StartKey, EndKeySel, Opts),
    lists:map(fun({_K, JobId}) -> JobId end, erlfdb:wait(Future)).


re_enqueue_inactive(#{jtx := true} = JTx, Type, Versionstamp) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    JobIds = get_inactive_since(JTx, Type, Versionstamp),
    lists:foreach(fun(JobId) ->
        Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
        {Seq, _, _, JobOpts} = get_job(Tx, Key),
        clear_activity(JTx, Type, Seq),
        maybe_enqueue(JTx, Type, JobId, JobOpts)
    end, JobIds),
    case length(JobIds) > 0 of
        true -> update_watch(JTx, Type);
        false -> ok
    end,
    JobIds.


% Debug and testing API

clear_type(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path := Jobs} = get_jtx(JTx),
    lists:foreach(fun(Section) ->
        Prefix = erlfdb_tuple:pack({Section, Type}, Jobs),
        erlfdb:clear_range_startswith(Tx, Prefix)
    end, [?DATA, ?PENDING, ?WATCHES, ?ACTIVITY_TIMEOUT, ?ACTIVITY]).


% Cache initialization API. Called from the supervisor just to create the ETS
% table. It returns `ignore` to tell supervisor it won't actually start any
% process, which is what we want here.
%
init_cache() ->
    ConcurrencyOpts = [{read_concurrency, true}, {write_concurrency, true}],
    ets:new(?MODULE, [public, named_table] ++ ConcurrencyOpts),
    ignore.


% Cached job transaction object. This object wraps a transaction, caches the
% directory lookup path, and the metadata version. The function can be used from
% or outside the transaction. When used from a transaction it will verify if
% the metadata was changed, and will refresh automatically.
%
get_jtx() ->
    get_jtx(undefined).


get_jtx(#{tx := Tx} = _TxDb) ->
    get_jtx(Tx);

get_jtx(undefined = _Tx) ->
    case ets:lookup(?MODULE, ?JOBS) of
        [{_, #{} = JTx}] -> JTx;
        [] -> update_jtx_cache(init_jtx(undefined))
    end;

get_jtx({erlfdb_transaction, _} = Tx) ->
    case ets:lookup(?MODULE, ?JOBS) of
        [{_, #{} = JTx}] -> ensure_current(JTx#{tx := Tx});
        [] -> update_jtx_cache(init_jtx(Tx))
    end.


% Transaction processing to be used with couch jobs' specific transaction
% contexts
%
tx(#{jtx := true} = JTx, Fun) when is_function(Fun, 1) ->
    fabric2_fdb:transactional(JTx, Fun).


% Utility fdb functions used by other module in couch_job. Maybe move these to a
% separate module if the list keep growing

has_versionstamp(?UNSET_VS) ->
    true;

has_versionstamp(Tuple) when is_tuple(Tuple) ->
    has_versionstamp(tuple_to_list(Tuple));

has_versionstamp([Elem | Rest]) ->
    has_versionstamp(Elem) orelse has_versionstamp(Rest);

has_versionstamp(_Other) ->
    false.


% Private helper functions

cancel(#{jx := true},  Key, {_, _, _, #{?OPT_CANCEL := true}}) ->
    ok;

cancel(#{jtx := true, tx := Tx}, Key, Job) ->
    {Seq, WorkerLockId, Priority, JobOpts} = Job,
    JobOpts1 = JobOpts#{?OPT_CANCEL => true},
    JobOptsEnc = jiffy:encode(JobOpts1),
    Val = erlfdb_tuple:pack({Seq, WorkerLockId, Priority, JobOptsEnc}),
    erlfdb:set(Tx, Key, Val),
    ok.


maybe_enqueue(#{jtx := true} = JTx, Type, JobId, JobOpts) ->
    #{tx := Tx, jobs_path := Jobs} = JTx,
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    Resubmit = maps:get(?OPT_RESUBMIT, JobOpts, false) == true,
    Cancel = maps:get(?OPT_CANCEL, JobOpts, false) == true,
    JobOpts1 = maps:without([?OPT_RESUBMIT], JobOpts),
    Priority = maps:get(?OPT_PRIORITY, JobOpts1, ?UNSET_VS),
    JobOptsEnc = jiffy:encode(JobOpts1),
    case Resubmit andalso not Cancel of
        true ->
            Val = erlfdb_tuple:pack_vs({null, null, Priority, JobOptsEnc}),
            case couch_jobs_util:has_versionstamp(Priority) of
                true -> erlfdb:set_versionstamped_value(Tx, Key, Val);
                false -> erlfdb:set(Tx, Key, Val)
            end,
            couch_jobs_pending:enqueue(JTx, Type, Priority, JobId);
        false ->
            Val = erlfdb_tuple:pack({null, null, null, JobOptsEnc}),
            erlfdb:set(Tx, Key, Val)
    end,
    ok.


get_job(Tx = {erlfdb_transaction, _}, Key) ->
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> = Val ->
            {Seq, WorkerLockId, Priority, JobOptsEnc} = erlfdb_tuple:unpack(Val),
            JobOpts = jiffy:decode(JobOptsEnc, [return_maps]),
            {Seq, WorkerLockId, Priority, JobOpts};
        not_found ->
            not_found
    end.


get_job_and_status(Tx, Key, WorkerLockId) ->
    case get_job(Tx, Key) of
        {_, LockId, _, _} = Res when WorkerLockId =/= LockId ->
            {worker_conflict, Res};
        {_, _, _, #{?OPT_CANCEL := true}} = Res ->
            {canceled, Res};
        {_, _, _, #{}} = Res ->
            {ok, Res};
        not_found ->
            {worker_conflict, not_found}
    end.


update_activity(#{jtx := true} = JTx, Type, JobId) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack_vs({?ACTIVITY, Type, ?UNSET_VS}, Jobs),
    erlfdb:set_versionstamped_key(Tx, Key, JobId).


clear_activity(#{jtx := true} = JTx, Type, Seq) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack({?ACTIVITY, Type, Seq}, Jobs),
    erlfdb:clear(Tx, Key).


update_watch(#{jtx := true} = JTx, Type) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack({?WATCHES, Type}, Jobs),
    erlfdb:set_versionstamped_value(Tx, Key, ?UNSET_VS).


update_lock(#{jtx := true} = JTx, Type, JobId, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  Jobs} = JTx,
    Key = erlfdb_tuple:pack({?DATA, Type, JobId}, Jobs),
    case get_job(Tx, Key) of
        {null, null, _, JobOpts} ->
            ValTup = {?UNSET_VS, WorkerLockId, null, jiffy:encode(JobOpts)},
            Val = erlfdb_tuple:pack_vs(ValTup),
            erlfdb:set_versionstamped_value(Tx, Key, Val);
        InvalidState ->
            error({couch_job_invalid_accept_state, InvalidState})
    end.


job_state(WorkerLockId, Priority) ->
    case {WorkerLockId, Priority} of
        {null, null} ->
            finished;
        {WorkerLockId, _} when WorkerLockId =/= null ->
            running;
        {_, Priority} when Priority =/= null ->
            pending;
        ErrorState ->
            error({invalid_job_state, ErrorState})
    end.





% This a transaction context object similar to the Db = #{} one from fabric2_fdb.
% It's is used to cache the jobs path directory (to avoid extra lookups on every
% operation) and to check for metadata changes (in case directory changes).
%
init_jtx(undefined) ->
    fabric2_fdb:transactional(fun(Tx) -> init_jtx(Tx) end);

init_jtx({erlfdb_transaction, _} = Tx) ->
    Root = erlfdb_directory:root(),
    CouchDB = erlfdb_directory:create_or_open(Tx, Root, [<<"couchdb">>]),
    LayerPrefix = erlfdb_directory:get_name(CouchDB),
    JobsPrefix = erlfdb_tuple:pack({?JOBS}, LayerPrefix),
    Version = erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)),
    % layer_prefix, md_version and tx here match db map fields in fabric2_fdb
    % but we also assert that this is a job transaction using the jtx => true field
    #{
        jtx => true,
        tx => Tx,
        layer_prefix => LayerPrefix,
        jobs_prefix => JobsPrefix,
        md_version => Version
    }.


ensure_current(#{jtx := true, tx := Tx, md_version := Version} = JTx) ->
    case erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)) of
        Version -> JTx;
        _NewVersion -> update_jtx_cache(init_jtx(Tx))
    end.


update_jtx_cache(#{jtx := true} = JTx) ->
    CachedJTx = JTx#{tx := undefined},
    ets:insert(?MODULE, {?JOBS, CachedJTx}),
    JTx.
