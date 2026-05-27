-module(pooler_pooled_worker_sup).

-behaviour(supervisor).

-export([start_link/1, start_link/2, init/1]).

-spec start_link(pooler:pool_config()) -> {ok, pid()} | {error, any()}.
start_link(#{start_mfa := _} = PoolConf) ->
    start_link(PoolConf, pooler_pool_sup:member_sup_name(PoolConf)).

-spec start_link(pooler:pool_config(), atom()) -> {ok, pid()} | {error, any()}.
start_link(#{start_mfa := MFA} = PoolConf, SupName) ->
    Shutdown = maps:get(member_shutdown, PoolConf, brutal_kill),
    supervisor:start_link({local, SupName}, ?MODULE, {MFA, Shutdown}).

init({Mod, Fun, Args}) when is_atom(Mod) ->
    %% Backward compat: old code passed just the MFA as init arg.
    %% Reached during hot upgrade from a release that predates member_shutdown.
    init({{Mod, Fun, Args}, brutal_kill});
init({{Mod, Fun, Args}, Shutdown}) ->
    Worker = #{
        id => Mod,
        start => {Mod, Fun, Args},
        restart => temporary,
        shutdown => Shutdown,
        type => worker,
        modules => [Mod]
    },
    Specs = [Worker],
    Restart = #{strategy => simple_one_for_one, intensity => 1, period => 1},
    {ok, {Restart, Specs}}.
