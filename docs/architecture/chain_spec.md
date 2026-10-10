# Chain Specification

Chain specification is used in Rundler to set chain specific parameters.

You can find the various parameters [here](../../crates/types/src/chain.rs).

Upon startup Rundler uses the following CLI params to gather the chain spec parameters:

* `--network`: Network name to lookup a hardcoded chain spec.
* `--chain_spec`: Path to a chain spec TOML file.
* `CHAIN_*`: Environment variables representing chain spec fields.

The chain specification is derived using the following steps:

### Find a `base` specification, if defined

Using the following config hierarchy:

- `CHAIN_BASE` env var
- `--chain_spec` file `base` key
- `--network` hardcoded spec `base` key

to find a chain spec base. A base is not required. A base must be a hardcoded network.

### Resolve the full chain spec

Using the following config hierarchy:

- `CHAIN_*` env vars
- `--chain_spec` file keys
- `--network` hardcoded spec keys
- base (if defined)
- defaults

to resolve the full chain spec. Only one level of `base` resolution is defined. That is, if a `base` network defined another `base`, the second `base` won't be resolved.

### Simulation sender

`simulation_sender` is the address that validation and gas estimation calls to the entry point are sent from, and so the `tx.origin` that accounts and paymasters see while Rundler simulates a user operation. It defaults to `0x0643866dA50efE0b055Cd15aF95191968c8411b5`, an address with no known private key that is the same for every Rundler deployment.

Set it when a paymaster or account restricts `tx.origin`, for example a paymaster that only sponsors operations bundled by a list of bundlers. Such a contract accepts a bundle sent by the bundle signer but rejects the simulation that precedes it, so `eth_estimateUserOperationGas` and `eth_sendUserOperation` fail with a validation revert (`AA33` for a paymaster) before anything is bundled. Setting `simulation_sender` to the bundle signer makes simulation use the `tx.origin` the bundle will have:

```
CHAIN_SIMULATION_SENDER=0x...
```

Or, in a chain spec file:

```toml
simulation_sender = "0x..."
```

The address needs no balance, as these calls set no fees. It must be an EOA without an EIP-7702 delegation.

### Hardcoded Chan Specs

See the files [here](../../bin/rundler/chain_specs/) for a list of hardcoded chain specifications.
