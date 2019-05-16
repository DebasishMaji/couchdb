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


% JobOpts field definitions
%
-define(OPT_PRIORITY, <<"priority">>).
-define(OPT_DATA, <<"data">>).
-define(OPT_CANCEL, <<"cancel">>).
-define(OPT_RESUBMIT, <<"resubmit">>).

% These might be in a fabric public .hrl eventually
%
-define(uint2bin(I), binary:encode_unsigned(I, little)).
-define(bin2uint(I), binary:decode_unsigned(I, little)).
-define(UNSET_VS, {versionstamp, 16#FFFFFFFFFFFFFFFF, 16#FFFF}).
-define(METADATA_VERSION_KEY, <<"$metadata_version_key$">>).

% Data model definitions
% Switch these to numbers eventually.
%
-define(JOBS, <<"couch_jobs">>).
-define(DATA, <<"data">>).
-define(PENDING, <<"pending">>).
-define(WATCHES, <<"watches">>).
-define(ACTIVITY_TIMEOUT, <<"activity_timeout">>).
-define(ACTIVITY, <<"activity">>).


% Couch jobs event notifier tag
-define(COUCH_JOBS_EVENT, '$couch_jobs_event').
