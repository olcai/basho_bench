%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2012 Erik Timan
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_fsal).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {fsalstate, id}).

%% We define the alphabet used for creating random path names
-define(ALPHABET, {$A,$B,$C,$D,$E,$F,$G,$H,$I,$J,$K,$L,$M,$N,$O,$P,
                   $Q,$R,$S,$T,$U,$V,$W,$X,$Y,$Z,$0,$1,$2,$3,$4,$5,
                   $6,$7,$8,$9}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure our File System Abstraction Layer application is
    %% available
    case code:which(fsal) of
        non_existing ->
            ?FAIL_MSG("~s requires fsal to be installed.\n", [?MODULE]);
        _ ->
            ok
    end,
    fsal:start(),
        
    Backend = basho_bench_config:get(backend),
    BackendArgs = basho_bench_config:get(backend_args),
    {ok, FSALState} = fsal:init([{backend, Backend},
                                 {backend_args, BackendArgs}]),

    %% Start the server that keep track of files to read/write. Set a
    %% generic output name for the table when saving it, and use the
    %% given input name when loading the table
    case filetable_server:start(
           [
            {filename, "my_filetable"},
            {input_filename, basho_bench_config:get(filetable_input)}
           ]) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok;
        Else -> erlang:error({could_not_start_filetable_server, Else})
    end,
    %% Let's walk the input file system
    Path = basho_bench_config:get(inputpath),
    ok = filetable_server:walk(Path),
    
    ?INFO("Starting worker with id ~p, using backend ~p", [Id, Backend]),
    %% For the first worker, print some system info
    case Id of
        1 ->
            ?INFO("System using kernel_poll: ~p, nbr of async threads: ~p",
                  [erlang:system_info(kernel_poll),
                   erlang:system_info(thread_pool_size)]);
        _ -> ok
    end,

    {ok, #state{fsalstate=FSALState, id=Id}}.

run(get, _KeyGen, _ValueGen, #state{fsalstate=FS, id=Id}=State) ->
    %% DANGER WILL ROBINSON! Will only work properly if all files in
    %% filetable_server have been putted before calling this function.
    TimeStart = os:timestamp(),
    case filetable_server:get_next_read() of
        none ->
            {stop, no_more_files_to_get};
        {file, _Prefix, RelPath, FileName, _Size, true} ->
            case fsal:get(RelPath, FileName, FS) of
                {ok, {file, Data}, NewFS} ->
                    TimeDiff = timer:now_diff(os:timestamp(), TimeStart),
                    ?DEBUG("get size ~p took ~p (~p)",
                           [size(Data), TimeDiff, Id]),
                    {ok, State#state{fsalstate=NewFS}};
                {ok, Error, NewFS} ->
                    {error,{Error},State#state{fsalstate=NewFS}}
            end
    end;
run(put, _KeyGen, ValueGen, #state{fsalstate=FS, id=Id}=State) ->
    %% The put operation is used when writing file contents directly
    %% (i.e. micro-benchmarks)
    TimeStart = os:timestamp(),
    {RelPath, FileName} = get_random_path(),
    Data = ValueGen(),
    case fsal:put(RelPath, FileName, Data, FS) of
        {ok, ok, NewFS} ->
            TimeDiff = timer:now_diff(os:timestamp(),
                                      TimeStart),
            ?DEBUG("put size ~p took ~p (~p)",
                   [size(Data), TimeDiff, Id]),
            %% We insert the file into filetable
            filetable_server:insert_file(RelPath,
                                         FileName,
                                         size(Data),
                                         true),
            {ok, State#state{fsalstate=NewFS}};
        {ok, Error, NewFS} ->
            {error,{Error},State#state{fsalstate=NewFS}}
    end;
run(put_direct, _KeyGen, _ValueGen, #state{fsalstate=FS, id=Id}=State) ->
    %% The put_direct operation is used to copy a source file to the
    %% destination without reading it into memory first.
    TimeStart = os:timestamp(),
    case filetable_server:get_next_write() of
        none ->
            {stop, no_more_files_to_put};
        {file, Prefix, RelPath, FileName, Size, _WriteFlag} ->
            SourceFile = filename_join([Prefix, RelPath, FileName]),
            case fsal:put_direct(RelPath, FileName, SourceFile, FS) of
                {ok, ok, NewFS} ->
                    TimeDiff = timer:now_diff(os:timestamp(), TimeStart),
                    ?DEBUG("put_direct size ~p took ~p (~p)",
                           [Size, TimeDiff, Id]),
                    {ok, State#state{fsalstate=NewFS}};
                {ok, Error, NewFS} ->
                    {error,{Error},State#state{fsalstate=NewFS}}
            end
    end;
run(delete, _KeyGen, _ValueGen, #state{fsalstate=FS, id=Id}=State) ->
    %% DANGER WILL ROBINSON! Will only work properly if all files in
    %% filetable_server have been putted before calling this function.
    TimeStart = os:timestamp(),
    case filetable_server:get_next_read() of
        none ->
            {stop, no_more_files_to_delete};
        {file, _Prefix, RelPath, FileName, Size, _WriteFlag} ->
            case fsal:delete(RelPath, FileName, FS) of
                {ok, ok, NewFS} ->
                    TimeDiff = timer:now_diff(os:timestamp(), TimeStart),
                    ?DEBUG("delete of size ~p took ~p (~p)",
                           [Size, TimeDiff, Id]),
                    {ok, State#state{fsalstate=NewFS}};
                {ok, Error, NewFS} ->
                    {error,{Error},State#state{fsalstate=NewFS}}
            end
    end.

%% ===================================================================
%% Internal function definitions
%% ===================================================================

%% Returns a {RelPath, FileName} tuple where the each part is randomly
%% created. Everything returned is binary.
get_random_path() ->
    {list_to_binary(random_str(2, ?ALPHABET)),
     list_to_binary(random_str(20, ?ALPHABET))}.

%% Return a random string of length Len using the alphabet given in
%% the tuple Chars.
random_str(0, _Chars) -> [];
random_str(Len, Chars) -> [random_char(Chars)|random_str(Len-1, Chars)].
random_char(Chars) -> element(random:uniform(tuple_size(Chars)), Chars).

%% These join functions should work like filename:join, except that
%% any intermediate dirs of type absolute won't overwrite the
%% preceeding elements. I.e. filename_join("aa", "/bb") will return
%% "aa/bb" NOT "/bb".
filename_join([Name1, Name2|Rest]) ->
    filename_join([filename_join(Name1, Name2)|Rest]);
filename_join([Name]) ->
    Name.

filename_join(<<>>, Name2) ->
    Name2;
filename_join([], Name2) ->
    Name2;
filename_join(Name1, Name2) ->
    case filename:pathtype(Name2) of
        absolute ->
            [_ | RelDir] = filename:split(Name2),
            filename:join([Name1] ++ RelDir);
        _ ->
            filename:join([Name1, Name2])
    end.
