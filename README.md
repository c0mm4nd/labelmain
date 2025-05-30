# LabelMain

🏷️ **Blockchain Address Label Collection Toolkit** - A comprehensive Go toolkit for collecting and managing blockchain address labels from multiple data sources

## 📖 Project Overview

LabelMain is a powerful blockchain data collection toolkit designed to gather and organize blockchain address label information from various reputable platforms. These labels are invaluable for:
- 🕵️ Blockchain analysis and investigation
- 🛡️ Security risk management and compliance
- 📊 Data analysis and research
- 🔍 Address attribution identification

## ✨ Supported Data Sources

| Tool Module | Data Source | Collection Content | Purpose |
|-------------|-------------|-------------------|---------|
| `bitcoinabuse` | [BitcoinAbuse.com](https://www.bitcoinabuse.com) | Bitcoin malicious address reports | Identify scam, ransomware, and other malicious addresses |
| `walletexplorer` | [WalletExplorer.com](https://www.walletexplorer.com) | Wallet service provider address labels | Identify exchange and service provider wallets |
| `chainabuse` | [ChainAbuse.com](https://www.chainabuse.com) | Multi-chain abuse address reports | Cross-chain malicious address detection |
| `tronscan` | [Tronscan.org](https://tronscan.org) | Tron network labels | Tron tokens, addresses, and contract information |
| `dappradar` | [DappRadar.com](https://dappradar.com) | DApp application information | Decentralized application data |
| `coincodex` | [CoinCodex.com](https://coincodex.com) | Cryptocurrency information | Token data and price history |
| `defillama` | [DefiLlama.com](https://defillama.com) | DeFi protocol information | DeFi ecosystem data |

## 🛠️ Requirements

### Essential Environment
- **Go 1.20+** - [Download and Install](https://golang.org/dl/)
- **MongoDB Atlas** - [Free Registration](https://www.mongodb.com/cloud/atlas) or local MongoDB instance

### API Keys (Optional)
Some data sources require API keys for better access limits:
- `DAPPRADAR_API_KEY_1` - DappRadar API key
- `TRONSCAN_API_KEY` - Tronscan API key

## ⚙️ Setup and Installation

### 1. Clone the Repository

```bash
git clone https://github.com/c0mm4nd/labelmain.git
cd labelmain
```

### 2. Configure Database Connection
Copy the environment variables example file and edit it:
```bash
cp .env.example .env
```

Edit the `.env` file and fill in your MongoDB connection string:
```bash
MONGO_URI=mongodb+srv://your_username:your_password@your_cluster.mongodb.net/?retryWrites=true&w=majority

# Optional: Other API keys
DAPPRADAR_API_KEY_1=your_DappRadar_API_key
TRONSCAN_API_KEY=your_Tronscan_API_key
```

### 3. Install Dependencies
```bash
go mod download
```

## 🚀 Usage

### Method 1: Direct Installation and Run (Recommended)

#### BitcoinAbuse Malicious Address Collection
```bash
# Install
go install github.com/c0mm4nd/labelmain/bitcoinabuse@latest

# Initialize run (first time use, collects historical data)
bitcoinabuse -init

# Daily run (collects only new data)
bitcoinabuse
```

#### WalletExplorer Wallet Label Collection
```bash
# Install
go install github.com/c0mm4nd/labelmain/walletexplorer@latest

# Run (continuously collects all wallet address data)
walletexplorer
```

#### Other Tools
```bash
# ChainAbuse abuse reports
go install github.com/c0mm4nd/labelmain/chainabuse@latest
chainabuse

# Tronscan data
go install github.com/c0mm4nd/labelmain/tronscan@latest  
tronscan

# DappRadar data
go install github.com/c0mm4nd/labelmain/dappradar@latest
dappradar

# CoinCodex data
go install github.com/c0mm4nd/labelmain/coincodex@latest
coincodex

# DefiLlama data  
go install github.com/c0mm4nd/labelmain/defillama@latest
defillama
```

### Method 2: Local Build and Run
```bash
# Build BitcoinAbuse tool
go build -o bitcoinabuse ./bitcoinabuse
./bitcoinabuse -init

# Build WalletExplorer tool  
go build -o walletexplorer ./walletexplorer
./walletexplorer

# Build other tools
go build -o chainabuse ./chainabuse
go build -o tronscan ./tronscan
# ... similar for other tools
```

## 📊 Data Storage Structure

All data is stored in MongoDB's `labels` database, organized by data source:

```
labels/
├── bitcoinLabels          # Bitcoin address labels
├── chainAbuse            # ChainAbuse abuse reports  
├── tronTokenLabels       # Tron token labels
├── tronAddressLabels     # Tron address labels
├── tronContractLabels    # Tron contract labels
├── dappradarTronLabels   # DappRadar Tron DApp data
├── coincodexAssetLabels  # CoinCodex asset data
├── coincodexDappLabels   # CoinCodex DApp data
└── defillamaLabels       # DefiLlama protocol data
```

## ⚠️ Important Notes

### Runtime Recommendations
- 🕒 **Long-running operation**: These tools are designed as long-term data collection services
- 🔄 **Auto-retry**: Built-in error retry and recovery mechanisms, automatically retry on network issues
- 💾 **Memory management**: Automatic memory cleanup when processing large amounts of data
- ⏱️ **Rate limiting**: Automatically handles API rate limits to avoid being banned

### Resource Consumption
- 💽 **Storage space**: Depending on collection scope, may require several GB to tens of GB of storage
- 🌐 **Network traffic**: Continuous network requests, recommended to run in stable network environment
- ⚡ **Processing time**: Initial runs may take several hours to days to collect historical data

### Compliance Usage
- 📋 Please comply with the terms of use and rate limits of each data source
- 🤝 Use only for legitimate research, analysis, and risk management purposes
- 🔒 Properly manage collected data and pay attention to data security

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Test specific modules
go test ./bitcoinabuse
go test ./walletexplorer
```

## 🤝 Contributing

Issues and Pull Requests are welcome!

## 📄 License

This project follows an open source license. Please see the LICENSE file for details.

---

**⭐ If this project helps you, please give it a Star!**
