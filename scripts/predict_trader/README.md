# trader-quickstart

A quickstart for the trader AI agent for AI prediction markets on Gnosis at https://github.com/valory-xyz/trader

## Resource Requirements

- You need xDAI on Gnosis Chain in one of your wallets.
- You need an RPC for your agent instance. We currently recommend providers [Quicknode](https://www.quicknode.com/) and [Chainstack](https://www.chainstack.com/).
- Some scripts may ask for a Subgraph API key that can be obtained at [The Graph](https://thegraph.com/studio/apikeys/).

## Observe your AI agents

1. Use the `trades` command to display information about placed trades by a given address:

    ```bash
    poetry run python -m scripts.predict_trader.trades --creator YOUR_SAFE_ADDRESS
    ```

    Or restrict the search to specific dates by defining the "from" and "to" dates:

    ```bash
    poetry run python -m scripts.predict_trader.trades --creator YOUR_SAFE_ADDRESS --from-date 2023-08-15:03:50:00 --to-date 2023-08-20:13:45:00
    ```

2. Use the `report` command to display a summary of the AI agent status:

   ```bash
   poetry run python -m scripts.predict_trader.report
   ```

3. Use this command to investigate your agent instance's logs:

    ```bash
    ./analyse_logs.sh configs/config_predict_trader.json --agent=aea_0 --reset-db
    ```

    For example, inspect the state transitions using this command:

    ```bash
    ./analyse_logs.sh configs/config_predict_trader.json --agent=aea_0 --reset-db --fsm
    ```

    This will output the different state transitions of your agent per period, for example:

    ![Trader FSM transitions](images/trader_fsm_transitions.png)

    For more options on the above command run:

    ```bash
    ./analyse_logs.sh --help
    ```

    or take a look at the [command documentation](https://docs.autonolas.network/open-autonomy/advanced_reference/commands/autonomy_analyse/#autonomy-analyse-logs).

## Advanced usage

This chapter is for advanced users who want to further customize the trader AI agent's behaviour without changing the underlying trading logic.

##### Tool selection

Sometimes, a mech tool might temporarily return invalid results.
As a result, the AI agent would end up performing mech calls without being able to use the response.
Assuming that this tool has a large reward rate in the policy weights,
the AI agent might end up spending a considerable amount of xDAI before adjusting the tool's reward rate,
without making any progress.
If a tool is temporarily misbehaving, you could set an environment variable as described below in order to exclude it.

##### Environment variables

You may customize the AI agent's behaviour by setting these trader-specific environment variables in [the config file](../../configs/config_predict_trader.json) in the same way as others.

| Name | Type | Default Value | Description |
| --- | --- | --- | --- |
| `ON_CHAIN_SERVICE_ID` | `int` | `null` | The ID of the on-chain AI agent. |
| `OMEN_CREATORS` | `list` | `["0x89c5cc945dd550BcFfb72Fe42BfF002429F46Fec"]` | The addresses of the market creator(s) that the AI agent will track. |
| `OPENING_MARGIN` | `int` | `300` | The markets opening before this margin will not be fetched. |
| `LANGUAGES` | `list` | `["en_US"]` | Filter questions by languages. |
| `SAMPLE_BETS_CLOSING_DAYS` | `int` | `10` | Sample the bets that are closed within this number of days. |
| `TRADING_STRATEGY` | `str` | `kelly_criterion_no_conf` | Trading strategy to use. |
| `USE_FALLBACK_STRATEGY` | `bool` | `true` | Whether to use the fallback strategy. |
| `BET_THRESHOLD` | `int` | `100000000000000000` | Threshold (wei) for placing a bet. A bet will only be placed if `potential_net_profit` - `BET_THRESHOLD` >= 0. |
| `PROMPT_TEMPLATE` | `str` | `With the given question "@{question}" and the 'yes' option represented by '@{yes}' and the 'no' option represented by '@{no}', what are the respective probabilities of 'p_yes' and 'p_no' occurring?` | The prompt template to use for prompting the mech. |
| `DUST_THRESHOLD` | `int` | `10000000000000` | Minimum amount (wei) below which a position's redeeming amount will be considered dust. |
| `POLICY_EPSILON` | `float` | `0.1` | Epsilon value for the e-Greedy policy for the tool selection based on tool accuracy. |
| `DISABLE_TRADING` | `bool` | `false` | Whether to disable trading. |
| `STOP_TRADING_IF_STAKING_KPI_MET` | `bool` | `true` | Whether to stop trading if the staking KPI is met. |
| `AGENT_BALANCE_THRESHOLD` | `int` | `10000000000000000` | Balance threshold (wei) below which the AI agent will stop trading and a refill will be required. |
| `REFILL_CHECK_INTERVAL` | `int` | `10` | Interval in seconds to check the AI agent balance, when waiting for a refill. |
| `FILE_HASH_TO_STRATEGIES_JSON` | `list` | `[["bafybeihufqu2ra7vud4h6g2nwahx7mvdido7ff6prwnib2tdlc4np7dw24",["bet_amount_per_threshold"]],["bafybeibxfp27rzrfnp7sxq62vwv32pdvrijxi7vzg7ihukkaka3bwzrgae",["kelly_criterion_no_conf"]]]` | A list of mapping from ipfs file hash to strategy names. |
| `STRATEGIES_KWARGS` | `list` | `[["bet_kelly_fraction",1.0],["floor_balance",500000000000000000],["bet_amount_per_threshold",{"0.0":0,"0.1":0,"0.2":0,"0.3":0,"0.4":0,"0.5":0,"0.6":60000000000000000,"0.7":90000000000000000,"0.8":100000000000000000,"0.9":1000000000000000000,"1.0":10000000000000000000}]]` | A list of keyword arguments for the strategies. |
| `USE_SUBGRAPH_FOR_REDEEMING` | `bool` | `true` | Whether to use the subgraph to check if a position is redeemed. |
| `USE_NEVERMINED` | `bool` | `false` | Whether to use Nevermined. |
| `SUBSCRIPTION_PARAMS` | `list` | `[["base_url", "https://marketplace-api.gnosis.nevermined.app/api/v1/metadata/assets/ddo"],["did", "did:nv:01706149da2f9f3f67cf79ec86c37d63cec87fc148f5633b12bf6695653d5b3c"],["escrow_payment_condition_address", "0x31B2D187d674C9ACBD2b25f6EDce3d2Db2B7f446"],["lock_payment_condition_address", "0x2749DDEd394196835199471027713773736bffF2"],["transfer_nft_condition_address", "0x659fCA7436936e9fe8383831b65B8B442eFc8Ea8"],["token_address", "0x1b5DeaD7309b56ca7663b3301A503e077Be18cba"], ["order_address","0x72201948087aE83f8Eac22cf7A9f2139e4cFA829"], ["nft_amount", "100"], ["payment_token","0x0000000000000000000000000000000000000000"], ["order_address", "0x72201948087aE83f8Eac22cf7A9f2139e4cFA829"],["price", "1000000000000000000"]]` | Parameters for the Nevermined subscription. |

The rest of the common environment variables are present in the [service.yaml](https://github.com/valory-xyz/trader/blob/v0.18.2/packages/valory/services/trader/service.yaml), which are customizable too.

To set `IRRELEVANT_TOOLS` for example, add the following lines under the `"env_variables"` field in the [the config file](../../configs/config_predict_trader.json).
```
...
        "IRRELEVANT_TOOLS": {
            "name": "",
            "description": "",
            "value": "[\"some-misbehaving-tool\",\"openai-text-davinci-002\",\"openai-text-davinci-003\",\"openai-gpt-3.5-turbo\",\"openai-gpt-4\",\"stabilityai-stable-diffusion-v1-5\",\"stabilityai-stable-diffusion-xl-beta-v2-2-2\",\"stabilityai-stable-diffusion-512-v2-1\",\"stabilityai-stable-diffusion-768-v2-1\"]",
            "provision_type": "fixed"
        },
...
```

##### Checking agent instance's health

You may check the health of the agent instance by querying the `/healthcheck` endpoint. For example:

```shell
curl -sL localhost:8716/healthcheck | jq -C
```

This will return a JSON output with the following fields:

| Field | Type | Criteria |
| --- | --- | --- |
| `seconds_since_last_transition` | float | The number of seconds passed since the last transition in the FSM. |
| `is_tm_healthy` | bool | `false` if more than `BLOCKS_STALL_TOLERANCE` (60) seconds have passed since the last begin block request received from the tendermint node. `true` otherwise. |
| `period` | int | The number of full cycles completed in the FSM. |
| `reset_pause_duration` | int | The number of seconds to wait before starting the next FSM cycle. |
| `rounds` | list | The last rounds (upto 25) in the FSM that happened including the current one. |
| `is_transitioning_fast` | bool | `true` if `is_tm_healthy` is `true` and `seconds_since_last_transition` is less than twice the `reset_pause_duration`. `false` otherwise. |

So, you can usually use `is_transitioning_fast` as a rule to check if an AI agent is healthy. To add a more strict check, you can also tune a threshold for the `seconds_since_last_transition` and rate of change of `period`, but that will require some monitoring to fine tune it.

##### Using a different priority mech with the mech marketplace

When running the AI agent for the first time, enter the mech address and AI agent ID of your choice when asked for `Priority Mech contract address` and `Priority Mech service ID`.
If you've already entered these values and want to change them later, follow this [guide](https://github.com/valory-xyz/quickstart/?tab=readme-ov-file#changing-entered-configuration)

## Migrate from trader-quickstart

If you were previously using [trader-quickstart](https://github.com/valory-xyz/trader-quickstart/tree/main) and want to migrate to the new unified [quickstart](https://github.com/valory-xyz/quickstart) repository, follow these steps:

> Note: Please ensure to meet the [system requirements](https://github.com/valory-xyz/quickstart/?tab=readme-ov-file#system-requirements) of this new quickstart.

1. Copy the `.trader_runner` folder from your trader-quickstart repository to the root of quickstart:

    ```bash
    cp -r /path/to/trader-quickstart/.trader_runner /path/to/quickstart/
    ```

2. Run the migration script to create the new `.operate` folder compatible with unified quickstart:

    ```bash
    poetry install
    poetry run python -m scripts.predict_trader.migrate_legacy_quickstart configs/config_predict_trader.json
    ```

3. Follow the prompts to complete the migration process. The script will:
   - Parse your existing configuration
   - Set up the new operate environment
   - Migrate your AI agent to the master safe
   - Handle any necessary unstaking and transfers

4. Once migration is complete, follow the instructions in the [Run the AI agent](../../README.md#run-the-ai-agent) section to run your trader AI agent.

5. After you ensure that the AI agent runs fine with the new quickstart, please delete the `.trader_runner` folder(s) to avoid any private key leaks.
