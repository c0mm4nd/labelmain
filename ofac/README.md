# OFAC Cryptocurrency Address Collector

This Go program fetches and parses the OFAC (Office of Foreign Assets Control) SDN (Specially Designated Nationals) list to extract sanctioned cryptocurrency addresses across multiple blockchains.

## Features

- Downloads the latest OFAC SDN Advanced XML file (~120MB)
- **Supports local file**: Automatically uses `sdn_advanced.xml` if present in the current directory
- Extracts cryptocurrency addresses for 17+ supported assets:
  - Bitcoin (XBT)
  - Ethereum (ETH)
  - Monero (XMR)
  - Litecoin (LTC)
  - Zcash (ZEC)
  - Dash (DASH)
  - Bitcoin Gold (BTG)
  - Ethereum Classic (ETC)
  - Bitcoin SV (BSV)
  - Bitcoin Cash (BCH)
  - Verge (XVG)
  - Tether (USDT)
  - Ripple (XRP)
  - Arbitrum (ARB)
  - Binance Smart Chain (BSC)
  - USD Coin (USDC)
  - Tron (TRX)
- Stores extracted data to MongoDB with entity names and metadata
- **JSON fallback**: Automatically exports to `ofac_addresses.json` if MongoDB fails
- Streaming XML parser for efficient memory usage (low memory footprint)

## Prerequisites

- Go 1.16 or higher
- MongoDB instance
- `.env` file with MongoDB connection string

## Installation

```bash
cd ofac
go mod tidy
```

## Configuration

Create or update the `.env` file in the project root:

```env
MONGO_URI=mongodb://username:password@host:port
```

For authentication-free local MongoDB:
```env
MONGO_URI=mongodb://localhost:27017
```

## Usage

### Option 1: Automatic Download

Run the program to automatically download from OFAC:

```bash
go run main.go
```

### Option 2: Manual Download (Recommended)

If automatic download is slow or fails, manually download the XML file first:

```bash
# Download the SDN Advanced XML file
curl -o sdn_advanced.xml "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ADVANCED.XML"

# Run the program (will automatically use the local file)
go run main.go
```

The program will:
1. Connect to MongoDB
2. Use local `sdn_advanced.xml` if available, otherwise download from OFAC
3. Parse and extract cryptocurrency addresses from 17+ blockchains
4. Store results to MongoDB `labels.ofacLabels` collection
5. If MongoDB fails, export to `ofac_addresses.json`

## Output

Each document in MongoDB contains:
- `address`: The cryptocurrency address
- `asset`: Asset symbol (XBT, ETH, etc.)
- `chain`: Blockchain name (Bitcoin, Ethereum, etc.)
- `entityName`: Name of the sanctioned entity
- `fileName`: Source file name (SDN_ADVANCED.XML)
- `publishDate`: Date when the data was collected
- `source`: Always "OFAC"
- `updatedAt`: Timestamp of last update

## Example Output

```
2025/11/12 02:37:04 Successfully connected to MongoDB!
2025/11/12 02:37:04 Downloading OFAC SDN XML file...
2025/11/12 02:37:10 Parsing XML data...
2025/11/12 02:37:10 Found feature type for XBT with ID 344
2025/11/12 02:37:10 Found feature type for ETH with ID 345
...
2025/11/12 02:38:37 Found 17 feature types and 18265 parties
2025/11/12 02:38:37 Extracted 748 cryptocurrency addresses
2025/11/12 02:38:37 Storing 748 addresses to MongoDB...
```

## MongoDB Permission Configuration

If you encounter authentication errors like `(Unauthorized) Command insert requires authentication`, configure MongoDB user permissions:

```javascript
// Connect to MongoDB
use admin

// Create or update user with appropriate permissions
db.createUser({
  user: "lab0",
  pwd: "your_password",
  roles: [
    { role: "readWrite", db: "labels" },
    { role: "dbAdmin", db: "labels" }
  ]
})

// Grant permissions to existing user
db.grantRolesToUser("lab0", [
  { role: "readWrite", db: "labels" },
  { role: "dbAdmin", db: "labels" }
])
```

Alternatively, the program will automatically fallback to JSON export if MongoDB write fails.

## Troubleshooting

### Network Timeout
If downloading the XML file times out, use manual download:
```bash
curl -o sdn_advanced.xml "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ADVANCED.XML"
```

### MongoDB Authentication Failed
- Check your `MONGO_URI` includes correct username and password
- Verify the user has `readWrite` permissions on the `labels` database
- The program will fallback to JSON export if MongoDB fails

### JSON Fallback
If MongoDB storage fails, data will be saved to `ofac_addresses.json` with the following structure:
```json
{
  "total_count": 748,
  "updated_at": "2025-11-12T02:50:03+08:00",
  "by_asset": {
    "XBT": 500,
    "ETH": 150,
    ...
  },
  "addresses": [...]
}
```

## Data Source

- **Source**: U.S. Department of the Treasury - Office of Foreign Assets Control
- **URL**: https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ADVANCED.XML
- **File Size**: ~120MB (as of November 2025)
- **Update Frequency**: OFAC updates this list regularly
- **Total Entities**: ~18,000+ sanctioned parties
- **Crypto Addresses**: ~748 addresses across 17+ blockchains (as of November 2025)

## License

This tool is for compliance and research purposes. Always refer to official OFAC sources for compliance decisions.
