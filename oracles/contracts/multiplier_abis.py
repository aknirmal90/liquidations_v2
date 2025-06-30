# ABIs for ratio provider methods used in multiplier calculations

# getRatio method - no parameters, returns uint256
GET_RATIO_ABI = [
    {
        "inputs": [],
        "name": "getRatio",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# convertToAssets method - takes uint256 parameter, returns uint256
CONVERT_TO_ASSETS_ABI = [
    {
        "inputs": [{"internalType": "uint256", "name": "shares", "type": "uint256"}],
        "name": "convertToAssets",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# getPooledEthByShares method - takes uint256 parameter, returns uint256
GET_POOLED_ETH_BY_SHARES_ABI = [
    {
        "inputs": [
            {"internalType": "uint256", "name": "sharesAmount", "type": "uint256"}
        ],
        "name": "getPooledEthByShares",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# rsETHPrice method - no parameters, returns uint256
RS_ETH_PRICE_ABI = [
    {
        "inputs": [],
        "name": "rsETHPrice",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# getExchangeRate method - no parameters, returns uint256
GET_EXCHANGE_RATE_ABI = [
    {
        "inputs": [],
        "name": "getExchangeRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# exchangeRate method - no parameters, returns uint256
EXCHANGE_RATE_ABI = [
    {
        "inputs": [],
        "name": "exchangeRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# getRate method - no parameters, returns uint256
GET_RATE_ABI = [
    {
        "inputs": [],
        "name": "getRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# chi method - no parameters, returns uint256
CHI_ABI = [
    {
        "inputs": [],
        "name": "chi",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

# Mapping of method names to their ABIs
METHOD_ABI_MAPPING = {
    "getRatio": GET_RATIO_ABI,
    "convertToAssets": CONVERT_TO_ASSETS_ABI,
    "getPooledEthByShares": GET_POOLED_ETH_BY_SHARES_ABI,
    "rsETHPrice": RS_ETH_PRICE_ABI,
    "getExchangeRate": GET_EXCHANGE_RATE_ABI,
    "exchangeRate": EXCHANGE_RATE_ABI,
    "getRate": GET_RATE_ABI,
    "chi": CHI_ABI,
}
