import pandas as pd
import requests
import time
from tqdm import tqdm

# # EVM-compatible chains
# def return_url_api(chain_name):
#     apis = {
#         'etherium': ("https://api.etherscan.io/api", "EFVFJ9B13JUFYH9DCPQT8H7H4TDP4KDPCU"),
#         'bsc': ("https://api.bscscan.com/api", "C6SFIUNDH3MMYEMFHWVA53PNCB9R38BX1U"),
#         'polygon': ("https://api.polygonscan.com/api", "QAMHGSIZWN217D1NQGITXPFTN1GE9A3CHX"),
#         'avalanche': ("https://api.snowtrace.io/api", ""),
#         'fantom': ("https://api.ftmscan.com/api", "C7HBH289BN99CXCMVEJ854XWE46YWBAZUC"),
#         'arbitrum': ("https://api.arbiscan.io/api", "VYBPGC7HXIACC33KVJ9U8H6DYXYDZQ1ZU1"),
#     }
#     return apis.get(chain_name, (None, None))

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
    
# Unix timestamps for start of 2023 and end of 2024
start_timestamp = 1735506000  # December 30, 2024, 00:00:00
end_timestamp = 1735549200    # December 30, 2024, 12:00:00

chains = ['etherium', 'bsc',  'polygon', 'fantom', 'arbitrum']

# Function to get block number by timestamp
def get_block_number_by_timestamp(timestamp, chain_api, chain_url):
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": timestamp,
        "closest": "before",
        "apikey": chain_api
    }
    response = requests.get(chain_url, params = params).json()
    if response["status"] == "1":
        return int(response["result"])
    else:
        raise Exception("Failed to fetch block number")

# Get unique wallet addresses for each chain 
for chain_name in chains:
    if chain_name != 'arbitrum':
        continue

    print(f"{chain_name = }")

    # Get chain url and api key
    chain_url, chain_api = return_url_api(chain_name)

    # Get block numbers for start and end of the range
    start_block = get_block_number_by_timestamp(start_timestamp, chain_api, chain_url)
    end_block = get_block_number_by_timestamp(end_timestamp, chain_api, chain_url)

    print(f"Chain: {chain_name}, Start Block: {start_block}, End Block: {end_block}")

    # Initialize tqdm progress bar
    total_blocks = end_block - start_block
    progress_bar = tqdm(total = total_blocks, desc = 'Fetching blocks', unit = 'block')
    
    # Unique wallets
    wallets = set()
    output_file = f"{chain_name}_wallet_addresses.txt"

    # Get wallet addresses from start to the end blocks and save them
    with open(output_file, "w") as f:

        # Set start block to current block
        current_block = start_block
        while current_block <= end_block:
            params = {
                "module": "proxy",
                "action": "eth_getBlockByNumber",
                "tag": hex(current_block),  # Convert block number to hex
                "boolean": "true",
                "apikey": chain_api
            }

            response = requests.get(chain_url, params = params).json()

            if "result" in response and response["result"]:
                transactions = response["result"]["transactions"]
                for tx in transactions:
                    from_wallet, to_wallet = tx['from'], tx['to']
                    try:    
                        if from_wallet not in wallets:
                            wallets.add(from_wallet)
                            f.write(from_wallet + "\n")
                        if to_wallet not in wallets:
                            wallets.add(to_wallet)
                            f.write(to_wallet + "\n")
                    except Exception as e:
                        print(f'Error processing address {from_wallet} or {to_wallet}: {e}\nGoing to the next transaction.')
                        continue

            current_block += 1  # Move to next block
            progress_bar.update(1)  # Update progress bar
            time.sleep(0.2)  # Avoid API rate limits

    progress_bar.close()  # Close the progress bar

    # wallets_list = list(wallets)
    # print(f"Collected {len(wallets_list)} wallet addresses.")

    # # Save to file
    # with open(f"{chain_name}_wallet_addresses.txt", "w") as f:
    #     for wallet in wallets_list:
    #         f.write(str(wallet) + "\n")

    print(f"Wallets saved to {chain_name}_wallet_addresses.txt")