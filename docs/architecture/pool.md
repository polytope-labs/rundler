# Pool Task

The `Pool` task is responsible for receiving, validating, sorting, and storing user operations. The `RPC` task submits the UOs from users and the `Builder` task consumes the UOs for bundling.

## Simulation

Upon each `add_operation` call the `Pool` will preforms a series of checks.

1. Run a series of [prechecks](https://eips.ethereum.org/EIPS/eip-4337#client-behavior-upon-receiving-a-useroperation) to catch any reasons why the UO may not be mined.

2. Simulate the UO via a `debug_traceCall` as per the [ERC-4337 spec](https://eips.ethereum.org/EIPS/eip-4337#simulation).

If violations are found, the UO is rejected. Else, the UO is added to the pool. We only accept User Operations into the pool if the `validUntil` field has over 60 seconds to expire from the time of entry or the `validAfter` field is before the time of entry.

### Tracer

A typescript based tracer is used to collect relevant information from the `debug_traceCall`. It is compiled into javascript in this repo and sent as a string as a parameter to the trace.

## Reputation

The `Pool` tracks the reputation of entities as per the [ERC-4337 spec](https://eips.ethereum.org/EIPS/eip-4337#reputation-scoring-and-throttlingbanning-for-global-entities).


### Allowlist/Blocklist

The `Pool` supports allowlists and blocklists configured via a JSON file. The JSON file must contain an array of addresses to add to the list.

Example file:
```
[
    "0xasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf"
]
```

**Allowlist**: Addresses on this list are always `Ok` in the reputation manager.

**Blocklist**: Addresses on this list are always `Banned` in the reputation manager.

### Paymaster Allowlist

With `--pool.paymaster_allowlist` set, the `Pool` only accepts UOs sponsored by a listed paymaster. It checks each UO submitted through `eth_sendUserOperation` before reputation, prechecks and simulation, and rejects it with JSON-RPC error `-32602` (invalid params) when:

- It has no paymaster: `user operation has no paymaster; this bundler only accepts operations sponsored by an allowed paymaster`.
- Its paymaster is not listed: `paymaster {address} is not allowed by this bundler`.

Notes:

- The same list applies to every enabled entry point version. List the address the UO names as its paymaster, i.e. the `paymaster` field (v0.7+) or the first 20 bytes of `paymasterAndData` (v0.6), which is the proxy address for a proxied paymaster.
- [Permissions](./rpc.md#user-operation-permissions) do not bypass the check. `trusted` UOs are rejected the same way, and `bundlerSponsorship` UOs are always rejected because they require an empty paymaster.
- `eth_estimateUserOperationGas` is unaffected.
- This list is separate from the reputation [allowlist](#allowlistblocklist), which only exempts addresses from reputation limits and rejects nothing.
- In a distributed deployment, run the same Rundler version for the `pool` or `backend` process and the `rpc` process, since the rejection reaches the RPC task as a new gRPC error variant. With mismatched versions the UO is still rejected, but an older `rpc` cannot decode the error and returns `-32603` instead.

## Chain Tracking

The `Pool` uses a JSON-RPC provider to track the progression of its chain. The chain tracker notifies the pool of new blocks, mined user operations, and "un-mined" user operations due to chain re-orgs.

Upon receiving a chain update event, the `Pool` will update its internal state by removing any mined user operations (and placing them in its cache), and by replacing any un-mined user operations (from its cache).

The `Pool`'s cache depth is configurable, if a re-org occurs that is deeper than the cache, UOs will be unable to be returned to the pool.

### Idle Pause

With `--pool.idle_pause_enabled`, the chain tracker stops polling the node while the bundler is idle. The pool and builder share an in-process idle gate, so this only works with the `node` and `backend` commands.

The tracker pauses after a chain update once all of these hold:

- No mempool has pending UOs, or mined UOs that a re-org within the cache depth could return.
- Nothing holds the gate. The builder holds it while work is in flight, see [triggers](./builder.md#triggers).
- `--pool.idle_pause_grace_millis` has passed since the last activity.

Any of these wakes it:

- An `AddOp`, i.e. a UO arriving through `eth_sendUserOperation`, unless the [paymaster allowlist](#paymaster-allowlist) rejects it.
- A sponsored delegation queued by the builder.
- A manual `debug_bundler_sendBundleNow`.

On wake the tracker resyncs from the current head only, without loading the blocks it missed while paused. The pool held no UOs at pause, so none depend on those blocks, but paymaster deposits and withdrawals in them go unseen. The pool therefore clears its cached paymaster balances and liabilities at pause, and on the resync it refetches every cached paymaster balance, as after a re-org deeper than the cache.

Metrics:

- `op_pool_chain_idle_paused`: gauge, `1` while paused.
- `op_pool_chain_idle_pauses`: count of pauses.
- `op_pool_chain_idle_resyncs`: count of resyncs from head after a pause.

## Mempool Config

Default operation of the mempool does not require an explicit configuration file. To use advanced mempool features like filtering and alternative mempool rules, users can specify a specific mempool configuration file using the `--mempool_config_path` CLI option. The schema for this JSON file can be found here: [MempoolConfigs](../../crates/sim/src/simulation/mempool.rs).

NOTE: only one mempool can be defined per entry point address at the moment.

Example config:

```
{
  "0x0000000000000000000000000000000000000000000000000000000000000000": {
    "description": "Allow list",
    "chainIds": ["0x066eed"],
    "filters": [
      {
        "id": "bls-aggregator",
        "filter": {
          "aggregator": "0x9d3a231e887a495ce6c454e7a38ed5e734bd5de4"
        }
      }
    ]
    "allowlist": [
      {
        "description": "My Factory",
        "rule": "notStaked",
        "entity": "0xasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf"
      }
    ]
  }
}
```

### Sharding

The `Pool` supports a very simple sharding scheme in its `best_operations` interface. The `Pool` is configured with a `num_shards` config, and the caller of `best_operations` provides a `shard_index` parameter.

User operations are assigned to a shard by their sender address modulo the number of shards.

Callers can use this feature to ensure that multiple callers are returned a disjoint set of user operations by sender. Callers should ensure that there is exactly 1 caller assigned to each shard index, else risk bundle invalidations (> 1 assigned) or orphaned user operations (0 assigned).

### Filtering

Advanced use cases may require mempool filtering. These filters MUST be used in conjunction with a [builder affinity](./builder.md#affinity) setting of the same filter ID, else the UOs will not be eligible for bundling.

These filters are used to tag each user operation with a filter ID as they enter the mempool. Builders can then match on this filter ID to have limit the user operations they receive to only those matching the filter.

Current filter implementations can be found in [MempoolFilter](../../crates/sim/src/simulation/mempool.rs).

### Alternative Mempools (in preview)

**NOTE: this feature presents known risks to the bundler, use at your own risk.**

The `Pool` supports configuring [alternative mempools](https://eips.ethereum.org/EIPS/eip-4337#alternative-mempools) via a JSON configuration file. This feature is under development with the community and will be modified soon.

See [here](https://hackmd.io/@dancoombs/BJYRz3h8n) for more details.

