# Import libraries
import requests
import os
import pandas as pd
from datetime import datetime
import time
from datetime import datetime
from tqdm import tqdm
import numpy as np

# Make directory function
def make_dir(path):
    if os.path.exists(path): pass
    else: os.mkdir(path)

# EVM-compatible chains
def return_url_api(chain_name):
    if chain_name == 'etherium':
        chain_url = "https://api.etherscan.io/api"
        chain_api = "EFVFJ9B13JUFYH9DCPQT8H7H4TDP4KDPCU"
        return chain_url, chain_api
    elif chain_name == 'bsc':
        chain_url = "https://api.bscscan.com/api"
        chain_api = "C6SFIUNDH3MMYEMFHWVA53PNCB9R38BX1U"
        return chain_url, chain_api
    elif chain_name == 'polygon':
        chain_url = "https://api.polygonscan.com/api"
        chain_api = "QAMHGSIZWN217D1NQGITXPFTN1GE9A3CHX"
        return chain_url, chain_api
    elif chain_name == 'avalanche':
        chain_url = "https://api.snowtrace.io/api"
        chain_api = ""
        return chain_url, chain_api
    elif chain_name == 'fantom':
        chain_url = "https://api.ftmscan.com/api"
        chain_api = "C7HBH289BN99CXCMVEJ854XWE46YWBAZUC"
        return chain_url, chain_api
    elif chain_name == 'arbitrum':
        chain_url = "https://api.arbiscan.io/api"
        chain_api = "VYBPGC7HXIACC33KVJ9U8H6DYXYDZQ1ZU1"
        return chain_url, chain_api

# Function to get wallet transactions
def get_wallet_transactions(wallet_address, chain_api, chain_url):
    params = {
        "module": "account",
        "action": "txlist",
        "address": wallet_address,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "asc",
        "apikey": chain_api
    }
    response = requests.get(chain_url, params = params)
    if response.status_code == 200:
        data = response.json()
        if data["status"] == "1":
            return data["result"]
        else:
            print("Error:", data["message"])
            return []
    else:
        print("Failed to fetch data")
        return []

# EVM-compatible chains
chains = ['etherium', 'bsc',  'polygon', 'fantom', 'arbitrum']

# Base wallet addresses path
wallet_addresses_path = r"E:\IBC\Project\wallet addresses"

# Transactions save path
save_transactions_path = r"E:\IBC\Project\transactions history"

# Num of wallets to process
start_wallet_id = 6413
end_wallet_id = 10000

# Get unique wallet addresses for each chain 
for chain_name in chains:
    if chain_name != 'arbitrum':
        continue

    print(f"{chain_name = }.")
	
    # Make directory for saving transactions
    save_chain_tr_path = os.path.join(save_transactions_path, chain_name)
    make_dir(save_chain_tr_path)

	# Read wallet addressess
    chain_wallet_addresses_path = os.path.join(wallet_addresses_path, f"{chain_name}_wallet_addresses.txt")
    chain_data = pd.read_csv(chain_wallet_addresses_path, names = ['wallet_addresses'])
    
    # Get limited wallet addresses
    chain_data = chain_data.iloc[start_wallet_id : end_wallet_id]

    # Saved transactions
    saved_transactions = os.listdir(os.path.join(save_transactions_path, chain_name))

	# Get chain url and api key
    chain_url, chain_api = return_url_api(chain_name)

	# Initialize tqdm progress bar
    progress_bar = tqdm(total = len(chain_data), desc = 'Fetching blocks', unit = 'block')

	# Get wallet transactions data
    for wallet_address in chain_data.wallet_addresses:
        if wallet_address in saved_transactions:
            continue

        # Get transactions
        wallet_transactions = get_wallet_transactions(wallet_address, chain_api, chain_url)

	    # Convert to dataframe
        wallet_transactions_df = pd.DataFrame(wallet_transactions)

        if not 'timeStamp' in wallet_transactions_df.columns:
            continue

	    # Convert data into readable format
        wallet_transactions_df['timeStamp_format'] = wallet_transactions_df['timeStamp'].astype(dtype = 'int64').apply(datetime.utcfromtimestamp)
        wallet_transactions_df['eth_value'] = wallet_transactions_df['value'].astype('float64').apply(lambda x: x / 1e18)
        wallet_transactions_df['gasPrice_gwei'] = wallet_transactions_df['gasPrice'].astype('float64').apply(lambda x: x / 1e9)

	    # Save transactions dataframe
        wallet_transactions_df.to_csv(f"{os.path.join(save_chain_tr_path, wallet_address)}.csv", index = False)       

        progress_bar.update(1)  # Update progress bar
        time.sleep(0.2)  # Avoid API rate limits

    progress_bar.close()  # Close the progress bar

    print(f"Wallet transactions for {chain_name} are saved.")