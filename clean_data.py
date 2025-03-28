# Import libraries
import requests
import os
import pandas as pd
from datetime import datetime
import time
from datetime import datetime
from tqdm import tqdm
import numpy as np
from web3 import Web3


# Make directory function
def make_dir(path):
    if os.path.exists(path): pass
    else: os.mkdir(path)

# EVM-compatible chains
def return_ankrrpc(chain_name):
    if chain_name == 'etherium':
        rpc = "https://rpc.ankr.com/eth/2ec89613290cab65c35f04fdd657226f0c66a92e2ce80e73e62d10ca70e1dee0"
        return rpc
    elif chain_name == 'bsc':
        rpc = "https://rpc.ankr.com/bsc/2ec89613290cab65c35f04fdd657226f0c66a92e2ce80e73e62d10ca70e1dee0"
        return rpc
    elif chain_name == 'polygon':
        rpc = "https://rpc.ankr.com/polygon/2ec89613290cab65c35f04fdd657226f0c66a92e2ce80e73e62d10ca70e1dee0"
        return rpc
    elif chain_name == 'avalanche':
        rpc = "https://rpc.ankr.com/avalanche/2ec89613290cab65c35f04fdd657226f0c66a92e2ce80e73e62d10ca70e1dee0"
        return rpc
    elif chain_name == 'fantom':
        rpc = "https://rpcapi.fantom.network/"
        return rpc
    elif chain_name == 'arbitrum':
        rpc = "https://arb1.arbitrum.io/rpc"
        return rpc

# Function to get wallet transactions
def is_contract(address, w3):
    # Check if an address is a contract.
    bytecode = w3.eth.get_code(Web3.to_checksum_address(address))
    return bool(bytecode)

# EVM-compatible chains
chains = ['etherium', 'bsc', 'polygon', 'fantom', 'arbitrum']

# Base addresses path
address_path = r"E:\IBC\Project\transactions history"

# Clean data save path
clean_data_path = r"E:\IBC\Project\clean data"

# Get unique wallet addresses for each chain
for chain_name in chains:
    if chain_name != 'fantom':
        continue

    print(f"{chain_name = }.")

    # Get Ankr RPC endpoint
    chain_ankrrpc = return_ankrrpc(chain_name)

    # Connect to the chain's RPC endpoint
    w3 = Web3(Web3.HTTPProvider(chain_ankrrpc))

	# Import addressess
    chain_address_path = os.path.join(address_path, chain_name)
    chain_addresses = os.listdir(chain_address_path)

    # Initialize tqdm progress bar
    progress_bar = tqdm(total = len(chain_addresses), desc = 'Processing addresses', unit = 'addresses')

    # Clean and processed data
    chain_clean_data = pd.DataFrame()
    import sys
	# Go through each address
    for chain_address in chain_addresses:
        # print(f"{chain_address[:-4]}")
	    
        # Check address
        address_status = is_contract(chain_address[:-4], w3)

        # Check if address is a contract
        if address_status:
            continue # If address is a contract, move to the next address

	    # Read dataframe
        wallet_transactions_df = pd.read_csv(os.path.join(chain_address_path, chain_address))

        # Drop transactions if ETH value is 0
        wallet_transactions_df = wallet_transactions_df[wallet_transactions_df['eth_value'] != 0]
        
        # Move to the next address if all transactions are 0
        if wallet_transactions_df.empty:
            continue

        # Consider the situation when wallet has sent ETH coins
        wallet_transactions_df = wallet_transactions_df[wallet_transactions_df['from'] == chain_address[:-4]]

        # Move to the next address if wallet has only received ETH coins
        if wallet_transactions_df.empty:
            continue

        # Re-index the dataframe
        wallet_transactions_df = wallet_transactions_df.reset_index(drop = True)

	    # Calculate necessary data
        start_time, end_time = wallet_transactions_df['timeStamp_format'].iloc[0], wallet_transactions_df['timeStamp_format'].iloc[-1]
        start_nonce, end_nonce = wallet_transactions_df['nonce'].astype(dtype = 'int64').iloc[0], wallet_transactions_df['nonce'].astype(dtype = 'int64').iloc[-1]
        total_eth_sent = np.sum(wallet_transactions_df['eth_value'])
        total_gas_utilized_gwei = np.sum(wallet_transactions_df['gasPrice_gwei'])

	    # Add data to clean dataframe
        clean_wallet_data = pd.DataFrame.from_dict({'wallet_address' : chain_address[:-4], 'start_time' : start_time, 'end_time' : end_time, 'start_nonce' : start_nonce,
                                            'end_nonce' : end_nonce, 'total_eth_sent' : total_eth_sent, 'total_gas_utilized_gwei' : total_gas_utilized_gwei}, orient = 'index').T
	    
        # Concatenate to chain clean data
        chain_clean_data = pd.concat([chain_clean_data, clean_wallet_data], axis = 0)  

        progress_bar.update(1)  # Update progress bar

    progress_bar.close()  # Close the progress bar

    # Save clean data
    chain_clean_data.reset_index(drop = True).to_csv(f'{chain_name}_clean_data.csv')
    print(f"Clean data for {chain_name} is saved.")