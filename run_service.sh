#!/bin/bash

set -e  # Exit script on first error

# Check if user is inside a venv
if [[ "$VIRTUAL_ENV" != "" ]]
then
    echo "Please exit the virtual environment!"
    exit 1
fi

# Check dependencies
command -v git >/dev/null 2>&1 ||
{ echo >&2 "Git is not installed!";
  exit 1
}

command -v poetry >/dev/null 2>&1 ||
{ echo >&2 "Poetry is not installed!";
  exit 1
}

command -v docker >/dev/null 2>&1 ||
{ echo >&2 "Docker is not installed!";
  exit 1
}

docker rm -f abci0 node0 trader_abci_0 trader_tm_0 &> /dev/null

store=".trader_runner"
rpc_path="$store/rpc.txt"
operator_keys_file="$store/operator_keys.json"
keys_json="keys.json"
keys_json_path="$store/$keys_json"
agent_address_path="$store/agent_address.txt"
service_id_path="$store/service_id.txt"

if [ -d $store ]; then
    first_run=false
    paths="$rpc_path $operator_keys_file $keys_json_path $agent_address_path $service_id_path"

    for file in $paths; do
        if ! [ -f "$file" ]; then
            echo "The runner's store is corrupted!"
            echo "Please manually investigate the $store folder"
            echo "Make sure that you do not lose your keys or any other important information!"
            exit 1
        fi
    done

    rpc=$(cat $rpc_path)
    agent_address=$(cat $agent_address_path)
    service_id=$(cat $service_id_path)
else
    first_run=true
    mkdir "$store"
fi

# Prompt for RPC
[[ -z "${rpc}" ]] && read -rsp "Enter a Gnosis RPC that supports eth_newFilter [hidden input]: " rpc && echo || rpc="${rpc}"

# Check if eth_newFilter is supported
new_filter_supported=$(curl -s -S -X POST \
  -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_newFilter","params":["invalid"],"id":1}' "$rpc" | \
  python3 -c "import sys, json; print(json.load(sys.stdin)['error']['message']=='The method eth_newFilter does not exist/is not available')")

if [ "$new_filter_supported" = True ]
then
    echo "The given RPC ($rpc) does not support 'eth_newFilter'! Terminating script..."
    exit 1
else
    echo -n "$rpc" > $rpc_path
fi

# clone repo
directory="trader"
# This is a tested version that works well.
# Feel free to replace this with a different version of the repo, but be careful as there might be breaking changes
service_version="v0.4.1"
service_repo=https://github.com/valory-xyz/$directory.git
if [ -d $directory ]
then
    echo "Detected an existing $directory repo. Using this one..."
    echo "Please stop and manually delete the $directory repo if you updated the service's version ($service_version)!"
    echo "You can run the following command, or continue with the pre-existing version of the service:"
    echo "rm -r $directory"
else
    echo "Cloning the $directory repo..."
    git clone --depth 1 --branch $service_version $service_repo
fi

cd $directory
if [ "$(git rev-parse --is-inside-work-tree)" = true ]
then
    poetry install
else
    echo "$directory is not a git repo!"
    exit 1
fi

gnosis_chain_id=100
n_agents=1

# setup the minting tool
export CUSTOM_CHAIN_RPC=$rpc
export CUSTOM_CHAIN_ID=$gnosis_chain_id
export CUSTOM_SERVICE_MANAGER_ADDRESS="0xE3607b00E75f6405248323A9417ff6b39B244b50"
export CUSTOM_SERVICE_REGISTRY_ADDRESS="0x9338b5153AE39BB89f50468E608eD9d764B755fD"
export CUSTOM_GNOSIS_SAFE_MULTISIG_ADDRESS="0x3C1fF68f5aa342D296d4DEe4Bb1cACCA912D95fE"

if [ "$first_run" = "true" ]
then
    # Generate the operator's key
    address_start_position=17
    pkey_start_position=21
    poetry run autonomy generate-key -n1 ethereum
    mv "$keys_json" "../$keys_json_path"
    operator_address=$(sed -n 3p "../$keys_json_path")
    operator_address=$(echo "$operator_address" | \
      awk '{ print substr( $0, '$address_start_position', length($0) - '$address_start_position' - 1 ) }')
    printf "Your operator's autogenerated public address: %s
    The same address will be used as the service owner.\n" "$operator_address"
    operator_pkey=$(sed -n 4p "../$keys_json_path")
    operator_pkey_file="operator_pkey.txt"
    echo -n "$operator_pkey" | awk '{ printf substr( $0, '$pkey_start_position', length($0) - '$pkey_start_position' ) }' > $operator_pkey_file
    mv "../$keys_json_path" "../$operator_keys_file"

    # Generate the agent's key
    poetry run autonomy generate-key -n1 ethereum
    mv "$keys_json" "../$keys_json_path"
    agent_address=$(sed -n 3p "../$keys_json_path")
    agent_address=$(echo "$agent_address" | \
      awk '{ print substr( $0, '$address_start_position', length($0) - '$address_start_position' - 1 ) }')
    private_key=$(sed -n 4p "../$keys_json_path")
    private_key=$(echo "$private_key" | \
      awk '{ print substr( $0, '$pkey_start_position', length($0) - '$pkey_start_position' ) }')
    echo "Your agent instance's autogenerated public address: $agent_address"
    echo -n "$agent_address" > "../$agent_address_path"

    # Check balances
    agent_balance=0
    operator_balance=0
    suggested_amount=50000000000000000
    until [[ $(python -c "print($agent_balance > ($suggested_amount-1))") == "True" && $(python -c "print($operator_balance > ($suggested_amount-1))") == "True" ]];
    do
        echo "Agent instance's balance: $agent_balance WEI."
        echo "Operator's balance: $operator_balance WEI."
        echo "Both of the addresses need to be funded to cover gas costs."
        echo "Please fund them with at least 0.05 xDAI each to continue."
        echo "Checking again in 10s..."
        sleep 10
        agent_balance=$(curl -s -S -X POST \
          -H "Content-Type: application/json" \
          --data '{"jsonrpc":"2.0","method":"eth_getBalance","params":["'"$agent_address"'","latest"],"id":1}' "$rpc" | \
          python3 -c "import sys, json; print(json.load(sys.stdin)['result'])")
        operator_balance=$(curl -s -S -X POST \
          -H "Content-Type: application/json" \
          --data '{"jsonrpc":"2.0","method":"eth_getBalance","params":["'"$operator_address"'","latest"],"id":1}' "$rpc" | \
          python3 -c "import sys, json; print(json.load(sys.stdin)['result'])")
        agent_balance=$((16#${agent_balance#??}))
        operator_balance=$((16#${operator_balance#??}))
    done

    echo "Minting your service on Gnosis chain..."

    # create service
    agent_id=12
    cost_of_bonding=10000000000000000
    nft="bafybeig64atqaladigoc3ds4arltdu63wkdrk3gesjfvnfdmz35amv7faq"
    service_id=$(poetry run autonomy mint \
      --skip-hash-check \
      --use-custom-chain \
      service packages/valory/services/$directory/ \
      --key "$operator_pkey_file" \
      --nft $nft \
      -a $agent_id \
      -n $n_agents \
      --threshold $n_agents \
      -c $cost_of_bonding
      )
    # parse only the id from the response
    service_id="${service_id##*: }"
    # validate id
    if ! [[ "$service_id" =~ ^[0-9]+$ || "$service_id" =~ ^[-][0-9]+$ ]]
    then
        echo "Service minting failed: $service_id"
        exit 1
    fi

    echo "[Service owner] Activating registration for service with id $service_id..."
    # activate service
    activation=$(poetry run autonomy service --use-custom-chain activate --key "$operator_pkey_file" "$service_id")
    # validate activation
    if ! [[ "$activation" = "Service activated succesfully" ]]
    then
        echo "Service registration activation failed: $activation"
        exit 1
    fi

    echo "[Service owner] Registering agent instance for service with id $service_id..."
    # register service
    registration=$(poetry run autonomy service --use-custom-chain register --key "$operator_pkey_file" "$service_id" -a $agent_id -i "$agent_address")
    # validate registration
    if ! [[ "$registration" = "Agent instance registered succesfully" ]]
    then
        echo "Service registration failed: $registration"
        exit 1
    fi

    echo "[Service owner] Deploying service with id $service_id..."
    # deploy service
    deployment=$(poetry run autonomy service --use-custom-chain deploy --key "$operator_pkey_file" "$service_id")
    # validate deployment
    if ! [[ "$deployment" = "Service deployed succesfully" ]]
    then
        echo "Service deployment failed: $deployment"
        exit 1
    fi

    # delete the operator's pkey file
    rm $operator_pkey_file
    # store service id
    echo -n "$service_id" > "../$service_id_path"
fi

# check state
expected_state="| Service State             | DEPLOYED                                     |"
service_info=$(poetry run autonomy service --use-custom-chain info "$service_id")
service_state=$(echo "$service_info" | grep "Service State")
if [ "$service_state" != "$expected_state" ]
then
    echo "Something went wrong while deploying the service. The service's state is:"
    echo "$service_state"
    echo "Please check the output of the script for more information."
    exit 1
else
    echo "$deployment"
fi

# get the deployed service's safe address from the contract
safe=$(echo "$service_info" | grep "Multisig Address")
address_start_position=31
safe=$(echo "$safe" |
  awk '{ print substr( $0, '$address_start_position', length($0) - '$address_start_position' - 3 ) }')
export SAFE_CONTRACT_ADDRESS=$safe

echo "Your service's safe address: $safe"

# Check the safe's balance
get_balance() {
    curl -s -S -X POST \
        -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_getBalance","params":["'"$SAFE_CONTRACT_ADDRESS"'","latest"],"id":1}' "$rpc" | \
        python3 -c "import sys, json; print(json.load(sys.stdin)['result'])"
}

convert_hex_to_decimal() {
    python3 -c "print(int('$1', 16))"
}

suggested_amount=500000000000000000
safe_balance_hex=$(get_balance)
safe_balance=$(convert_hex_to_decimal $safe_balance_hex)
while [ "$(python -c "print($safe_balance < $suggested_amount)")" == "True" ]; do
    echo "Safe's balance: $safe_balance WEI."
    echo "The safe address needs to be funded."
    echo "Please fund it with the amount you want to use for trading (at least 0.5 xDAI) to continue."
    echo "Checking again in 10s..."
    sleep 10
    safe_balance_hex=$(get_balance)
    safe_balance=$(convert_hex_to_decimal $safe_balance_hex)
done

# Set environment variables. Tweak these to modify your strategy
export RPC_0="$rpc"
export CHAIN_ID=$gnosis_chain_id
export ALL_PARTICIPANTS='["'$agent_address'"]'
# This is the default market creator. Feel free to update with other market creators
export OMEN_CREATORS='["0x89c5cc945dd550BcFfb72Fe42BfF002429F46Fec"]'
export BET_AMOUNT_PER_THRESHOLD_000=0
export BET_AMOUNT_PER_THRESHOLD_010=0
export BET_AMOUNT_PER_THRESHOLD_020=0
export BET_AMOUNT_PER_THRESHOLD_030=0
export BET_AMOUNT_PER_THRESHOLD_040=0
export BET_AMOUNT_PER_THRESHOLD_050=0
export BET_AMOUNT_PER_THRESHOLD_060=30000000000000000
export BET_AMOUNT_PER_THRESHOLD_070=40000000000000000
export BET_AMOUNT_PER_THRESHOLD_080=60000000000000000
export BET_AMOUNT_PER_THRESHOLD_090=80000000000000000
export BET_AMOUNT_PER_THRESHOLD_100=100000000000000000
export BET_THRESHOLD=5000000000000000
export PROMPT_TEMPLATE="With the given question \"@{question}\" and the \`yes\` option represented by \`@{yes}\` and the \`no\` option represented by \`@{no}\`, what are the respective probabilities of \`p_yes\` and \`p_no\` occurring?"

service_dir="trader_service"
build_dir="abci_build"
directory="$service_dir/$build_dir"
if [ -d $directory ]
then
    echo "Detected an existing build. Using this one..."
    cd $service_dir
    echo "You will need to provide sudo password in order for the script to delete part of the build artifacts."
    sudo rm -rf $build_dir
else
    echo "Setting up the service..."

    if ! [ -d "$service_dir" ]; then
        # Fetch the service
        poetry run autonomy fetch --local --service valory/trader --alias $service_dir
    fi

    cd $service_dir
    # Build the image
    poetry run autonomy build-image
    cp ../../$keys_json_path $keys_json
fi

# Build the deployment with a single agent
poetry run autonomy deploy build --n $n_agents -ltm

cd ..

# Run the deployment
poetry run autonomy deploy run --build-dir $directory --detach
