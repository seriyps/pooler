%% @author Seth Falcon <seth@userprimary.net>
%% @copyright 2012-2013 Seth Falcon
%% @doc Helper gen_server to manage async member lifecycle (start and stop)
%%
-module(pooler_starter).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% Legacy placeholder: misnomer — resolves to the (shard-specific) member sup name,
%% not the pool name. Kept as a deprecated alias for ?POOLER_MEMBER_SUP for backward
%% compatibility with existing user-provided stop_mfa configurations.
-define(POOLER_POOL_NAME, '$pooler_pool_name').
%% Shard-specific member supervisor name. Use this in new stop_mfa configurations.
-define(POOLER_MEMBER_SUP, '$pooler_member_sup').
%% The pool name atom. Use this when stop_mfa needs to look up pool state.
-define(POOLER_POOL, '$pooler_pool').
-define(POOLER_PID, '$pooler_pid').
-define(DEFAULT_STOP_MFA, {supervisor, terminate_child, [?POOLER_MEMBER_SUP, ?POOLER_PID]}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
    start_link/1,
    start_member/2,
    start_member/3,
    start_member/4,
    stop_member_async/1,
    stop/1,
    stop_spec/4,
    default_stop_mfa/0,
    replace_placeholders/4
]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([start_spec/0, start_result/0, stop_spec/0, stop_mfa/0]).

-type pool_member_sup() :: pid() | atom().
-type parent() :: pid() | pool.
-type initialize_mfa() :: undefined | {module(), atom(), [term()]}.
-type stop_mfa() ::
    {module(), atom(), ['$pooler_pid' | '$pooler_member_sup' | '$pooler_pool' | '$pooler_pool_name' | term()]}.
-type start_result() :: {StarterPid :: pid(), Result :: pid() | {error, _}}.
-opaque start_spec() :: {starter_spec, pooler:pool_name(), pool_member_sup(), parent(), initialize_mfa()}.
-opaque stop_spec() :: {stopper_spec, pooler:pool_name(), pool_member_sup(), pid(), stop_mfa()}.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-spec start_link(start_spec() | stop_spec()) -> {ok, pid()}.
start_link({starter_spec, _, _, _, _} = Spec) ->
    gen_server:start_link(?MODULE, Spec, []);
start_link({stopper_spec, _, _, _, _} = Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

stop(Starter) ->
    gen_server:cast(Starter, stop).

%% @doc Start a member for the specified `Pool'.
%%
%% Member creation with this call is async. This function returns
%% immediately with create process' pid. When the member has been
%% created it is sent to the specified pool via
%% {@link pooler:accept_member/2}.
%%
%% Each call starts a single use `pooler_starter' instance via
%% `pooler_starter_sup'. The instance terminates normally after
%% creating a single member.
-spec start_member(pooler:pool_name(), pool_member_sup()) -> pid().
start_member(PoolName, PoolMemberSup) ->
    start_member(PoolName, PoolMemberSup, undefined).

-spec start_member(pooler:pool_name(), pool_member_sup(), initialize_mfa()) -> pid().
start_member(PoolName, PoolMemberSup, InitMFA) ->
    {ok, Pid} = pooler_starter_sup:new_starter({starter_spec, PoolName, PoolMemberSup, pool, InitMFA}),
    Pid.

%% @doc Same as {@link start_member/2} except that instead of calling
%% {@link pooler:accept_member/2} a raw message is sent to `Parent' of
%% the form `{accept_member, {Ref, Member}'. Where `Member' will
%% either be the member pid or an error term and `Ref' will be the
%% Pid of the starter.
%%
%% This is used by the init function in the `pooler' to start the
%% initial set of pool members in parallel.
-spec start_member(pooler:pool_name(), pool_member_sup(), pid(), initialize_mfa()) -> pid().
start_member(PoolName, PoolMemberSup, Parent, InitMFA) ->
    {ok, Pid} = pooler_starter_sup:new_starter({starter_spec, PoolName, PoolMemberSup, Parent, InitMFA}),
    Pid.

%% @doc Stop a member in the pool

%% Member creation can take too long. In this case, the starter
%% needs to be informed that even if creation succeeds, the
%% started child should be not be sent back and should be
%% cleaned up
-spec stop_member_async(pid()) -> ok.
stop_member_async(Pid) ->
    gen_server:cast(Pid, stop_member).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
-record(starter, {
    %% start mode
    parent :: parent() | undefined,
    pool_name :: pooler:pool_name(),
    pool_member_sup :: pool_member_sup() | undefined,
    initialize_mfa :: initialize_mfa(),
    msg :: start_result() | undefined,
    %% stop mode
    stopping_pid = undefined :: pid() | undefined,
    stopping_mfa = undefined :: stop_mfa() | undefined
}).

-spec init(start_spec() | stop_spec()) -> {ok, #starter{}, {continue, start | stop}}.
init({stopper_spec, PoolName, MemberSup, MemberPid, StopMFA}) ->
    {ok,
        #starter{
            pool_name = PoolName,
            pool_member_sup = MemberSup,
            stopping_pid = MemberPid,
            stopping_mfa = StopMFA
        },
        {continue, stop}};
init({starter_spec, PoolName, PoolMemberSup, Parent, InitMFA}) ->
    {ok, #starter{pool_name = PoolName, pool_member_sup = PoolMemberSup, parent = Parent, initialize_mfa = InitMFA},
        {continue, start}}.

handle_continue(
    start,
    #starter{pool_member_sup = PoolSup, pool_name = PoolName, initialize_mfa = InitMFA} = State
) ->
    Msg = do_start_member(PoolSup, PoolName, InitMFA),
    % asynchronously in order to receive potential `stop*'
    accept_member_async(self()),
    {noreply, State#starter{msg = Msg}};
handle_continue(
    stop,
    #starter{pool_name = PoolName, pool_member_sup = MemberSup, stopping_pid = MemberPid, stopping_mfa = StopMFA} =
        State
) ->
    terminate_pid(PoolName, MemberSup, MemberPid, StopMFA),
    {stop, normal, State}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(stop_member, #starter{msg = {_Me, Pid}, pool_member_sup = MemberSup} = State) when is_pid(Pid) ->
    %% The process we were starting is no longer valid for the pool.
    %% Cleanup the process and stop normally.
    supervisor:terminate_child(MemberSup, Pid),
    {stop, normal, State};
handle_cast(stop_member, State) ->
    %% Either init failed (msg is an error) or start_member hasn't completed yet.
    {stop, normal, State};
handle_cast(accept_member, #starter{msg = Msg, parent = Parent, pool_name = PoolName} = State) ->
    %% Process creation has succeeded. Send the member to the pooler
    %% gen_server to be accepted. Pooler gen_server will notify
    %% us if the member was accepted or needs to cleaned up.
    send_accept_member(Parent, PoolName, Msg),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_start_member(PoolSup, PoolName, InitMFA) ->
    case supervisor:start_child(PoolSup, []) of
        {ok, Pid} ->
            case call_initialize_mfa(PoolName, PoolSup, Pid, InitMFA) of
                ok ->
                    {self(), Pid};
                Error ->
                    ?LOG_ERROR(
                        #{
                            label => "failed to initialize member",
                            pool => PoolName,
                            pid => Pid,
                            error => Error
                        },
                        #{domain => [pooler]}
                    ),
                    supervisor:terminate_child(PoolSup, Pid),
                    {self(), Error}
            end;
        Error ->
            ?LOG_ERROR(
                #{
                    label => "failed to start member",
                    pool => PoolName,
                    error => Error
                },
                #{domain => [pooler]}
            ),
            {self(), Error}
    end.

-spec default_stop_mfa() -> stop_mfa().
default_stop_mfa() ->
    ?DEFAULT_STOP_MFA.

-spec stop_spec(pooler:pool_name(), pool_member_sup(), pid(), stop_mfa()) -> stop_spec().
stop_spec(PoolName, MemberSup, MemberPid, StopMFA) ->
    {stopper_spec, PoolName, MemberSup, MemberPid, StopMFA}.

%% @doc Best-effort termination for a pool member: applies the given MFA with
%% `?POOLER_PID', `?POOLER_MEMBER_SUP', `?POOLER_POOL', and (legacy)
%% `?POOLER_POOL_NAME' placeholders replaced by the actual values. Falls back
%% to the default stop MFA on any failure.
-spec terminate_pid(pooler:pool_name(), pool_member_sup(), pid(), stop_mfa()) -> ok.
terminate_pid(PoolName, MemberSup, Pid, {Mod, Fun, Args}) when is_list(Args) ->
    NewArgs = replace_placeholders(PoolName, MemberSup, Pid, Args),
    try erlang:apply(Mod, Fun, NewArgs) of
        _ -> ok
    catch
        _:_ -> terminate_pid(PoolName, MemberSup, Pid, ?DEFAULT_STOP_MFA)
    end.

-spec replace_placeholders(pooler:pool_name(), pool_member_sup(), pid(), [term()]) -> [term()].
replace_placeholders(PoolName, MemberSup, Pid, Args) ->
    [
        case Arg of
            ?POOLER_MEMBER_SUP -> MemberSup;
            %% Legacy alias — semantically same as ?POOLER_MEMBER_SUP. The original
            %% name was a misnomer (it never resolved to the pool name).
            ?POOLER_POOL_NAME -> MemberSup;
            ?POOLER_POOL -> PoolName;
            ?POOLER_PID -> Pid;
            _ -> Arg
        end
     || Arg <- Args
    ].

-spec call_initialize_mfa(pooler:pool_name(), pool_member_sup(), pid(), initialize_mfa()) -> ok | {error, term()}.
call_initialize_mfa(_PoolName, _MemberSup, _Pid, undefined) ->
    ok;
call_initialize_mfa(PoolName, MemberSup, Pid, {Mod, Fun, Args}) ->
    NewArgs = replace_placeholders(PoolName, MemberSup, Pid, Args),
    try erlang:apply(Mod, Fun, NewArgs) of
        ok -> ok;
        {error, _} = Err -> Err;
        Other -> {error, {unexpected_initialize_mfa_return, Other}}
    catch
        _:Reason -> {error, {initialize_mfa_exit, Reason}}
    end.

-spec send_accept_member(parent(), pooler:pool_name(), start_result()) -> ok.
send_accept_member(pool, PoolName, Msg) ->
    %% used to grow pool
    pooler:accept_member(PoolName, Msg);
send_accept_member(Pid, _PoolName, Msg) ->
    %% used during pool initialization
    Pid ! {accept_member, Msg},
    ok.

accept_member_async(Pid) ->
    gen_server:cast(Pid, accept_member).
