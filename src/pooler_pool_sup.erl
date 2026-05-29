-module(pooler_pool_sup).

-behaviour(supervisor).

-export([
    start_link/1,
    init/1,
    pool_sup_name/1,
    member_sup_name/1,
    member_sup_names/2,
    build_member_sup_name/1,
    add_member_sups/4
]).

-spec start_link(pooler:pool_config()) -> {ok, pid()}.
start_link(PoolConf) ->
    SupName = pool_sup_name(PoolConf),
    supervisor:start_link({local, SupName}, ?MODULE, PoolConf).

init(PoolConf) when is_map(PoolConf) ->
    PoolerSpec = #{
        id => pooler,
        start => {pooler, start_link, [PoolConf]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [pooler]
    },
    PoolName = maps:get(name, PoolConf),
    N = maps:get(num_member_sups, PoolConf, 1),
    MemberSupSpecs = [
        begin
            SupName = member_sup_name(PoolName, I, N),
            #{
                id => SupName,
                start => {pooler_pooled_worker_sup, start_link, [PoolConf, SupName]},
                restart => transient,
                shutdown => 5000,
                type => supervisor,
                modules => [pooler_pooled_worker_sup]
            }
        end
     || I <- lists:seq(1, N)
    ],
    %% five restarts in 60 seconds, then shutdown
    Restart = #{strategy => one_for_all, intensity => 5, period => 60},
    {ok, {Restart, MemberSupSpecs ++ [PoolerSpec]}};
init(PoolRecord) when is_tuple(PoolRecord), element(1, PoolRecord) =:= pool ->
    %% This clause is for the hot code upgrade from pre-1.6.0;
    %% can be removed when "upgrade-from-version" below 1.6.0 are removed from `pooler.appup.src'
    {ok, PoolRecord1} = pooler:code_change(0, PoolRecord, []),
    AsMap = pooler:to_map(PoolRecord1),
    init(
        maps:with(
            [
                name,
                init_count,
                max_count,
                start_mfa,
                group,
                cull_interval,
                max_age,
                member_start_timeout,
                queue_max,
                metrics_api,
                metrics_mod,
                stop_mfa,
                initialize_mfa,
                auto_grow_threshold,
                add_member_retry,
                metrics_mod,
                metrics_api
            ],
            AsMap
        )
    ).

-spec member_sup_name(pooler:pool_config()) -> atom().
member_sup_name(#{name := Name}) ->
    build_member_sup_name(Name).

%% @doc Return the member sup name for shard `I' of `N' total shards.
%% Shard 1 always uses the legacy unsuffixed name (`pooler_<pool>_member_sup')
%% regardless of N, for consistency: a pool that grows from N=1 to N=2 via
%% reconfigure keeps its original shard-1 supervisor name unchanged. Additional
%% shards use `_2', `_3', ... suffixes.
-spec member_sup_name(pooler:pool_name(), pos_integer(), pos_integer()) -> atom().
member_sup_name(PoolName, 1, _N) ->
    build_member_sup_name(PoolName);
member_sup_name(PoolName, I, _N) ->
    build_member_sup_shard_name(PoolName, I).

%% @doc Build the tuple of member supervisor names for a pool with `N' shards.
%% With N=1, returns a 1-tuple with the legacy unsuffixed name for backward compatibility.
-spec member_sup_names(pooler:pool_name(), pos_integer()) -> tuple().
member_sup_names(PoolName, N) ->
    list_to_tuple([member_sup_name(PoolName, I, N) || I <- lists:seq(1, N)]).

-spec build_member_sup_name(pooler:pool_name()) -> atom().
build_member_sup_name(PoolName) ->
    list_to_atom("pooler_" ++ atom_to_list(PoolName) ++ "_member_sup").

-spec build_member_sup_shard_name(pooler:pool_name(), pos_integer()) -> atom().
build_member_sup_shard_name(PoolName, I) ->
    list_to_atom("pooler_" ++ atom_to_list(PoolName) ++ "_member_sup_" ++ integer_to_list(I)).

%% @doc Start additional member supervisors for shards `OldN+1..NewN' under
%% the pool's pool_sup. Used by `pooler:pool_reconfigure/2' when `num_member_sups'
%% is increased. New shards use `_I'-suffixed names (I >= 2; shard 1 is reserved
%% for the legacy unsuffixed name which already exists at this point).
-spec add_member_sups(pooler:pool_name(), {atom(), atom(), [term()]}, pos_integer(), pos_integer()) -> [atom()].
add_member_sups(PoolName, StartMFA, OldN, NewN) when NewN > OldN ->
    PoolSupName = pool_sup_name(#{name => PoolName}),
    NewNames = [build_member_sup_shard_name(PoolName, I) || I <- lists:seq(OldN + 1, NewN)],
    lists:foreach(
        fun(SupName) ->
            Spec = #{
                id => SupName,
                start => {pooler_pooled_worker_sup, start_link, [#{start_mfa => StartMFA}, SupName]},
                restart => transient,
                shutdown => 5000,
                type => supervisor,
                modules => [pooler_pooled_worker_sup]
            },
            case supervisor:start_child(PoolSupName, Spec) of
                {ok, _} -> ok;
                {error, Reason} -> error({failed_to_add_member_sup, SupName, Reason})
            end
        end,
        NewNames
    ),
    NewNames.

pool_sup_name(#{name := Name}) ->
    list_to_atom("pooler_" ++ atom_to_list(Name) ++ "_pool_sup").
