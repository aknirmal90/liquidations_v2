# Liquidations V2 Application

## Overview
This application provides comprehensive blockchain event monitoring and price oracle management for DeFi liquidation tracking. It consists of two main modules: blockchains (core event handling) and oracles (price feed management).

# Blockchains Module

## Overview
The blockchains module provides core blockchain event handling infrastructure for the liquidations application. It manages blockchain event synchronization, initialization, and data persistence.

## Models

### `Event` (models.py:4)
- **Purpose**: Represents blockchain events that need to be monitored and synchronized
- **Inherits from**: `BaseEvent` (utils.models.py)
- **Key fields**:
  - `name`: Event name (e.g., "Transfer", "Liquidation")
  - `signature`: Event signature for identification
  - `abi`: JSON ABI definition for event parsing
  - `topic_0`: Event topic hash for filtering
  - `contract_addresses`: List of contract addresses to monitor
  - `last_synced_block`: Last synchronized block number
  - `logs_count`: Number of logs processed for this event
  - `is_enabled`: Whether event synchronization is active

### `BaseEvent` (utils/models.py:6)
- **Purpose**: Abstract base class for all blockchain events
- **Features**:
  - Automatic timestamping (created_at, updated_at)
  - Block synchronization tracking
  - ClickHouse column mapping for EVM types
  - Property to calculate blocks remaining to sync

## Tasks

### `InitializeAppTask` (tasks.py:28)
- **Purpose**: Initialize the application by setting up protocol events and database structure
- **Operations**:
  - Creates ClickHouse database
  - Reads protocol configuration and ABI files
  - Creates Event instances for each configured event
  - Sets up materialized views for both blockchains and oracles modules
- **Configuration**: Uses `PROTOCOL_CONFIG_PATH` and `PROTOCOL_ABI_PATH` constants

### `ResetAppTask` (tasks.py:152)
- **Purpose**: Reset the application by cleaning all data
- **Operations**:
  - Drops ClickHouse database
  - Deletes all Event instances
  - Deletes all PriceEvent instances
  - Clears Redis cache

### `ChildSynchronizeTask` (tasks.py:181)
- **Purpose**: Synchronize individual blockchain events
- **Mixins**: `EventSynchronizeMixin`
- **Components**: Uses `clickhouse_client` and `rpc_adapter` for data operations

### `ParentSynchronizeTask` (tasks.py:191)
- **Purpose**: Orchestrate multiple child synchronization tasks
- **Mixins**: `ParentSynchronizeTaskMixin`
- **Relationship**: Manages `ChildSynchronizeTask` instances

## Materialized Views
The module includes SQL materialized views in `mv_queries/` directory:
- Token metadata views (101-106)
- Latest configuration views (201-206)
- Materialized view definitions (301-306)
- Asset configuration view (990)

## Key Dependencies
- **ClickHouse**: For high-performance event data storage
- **Celery**: For asynchronous task processing
- **Django ORM**: For application data management
- **Web3/eth_utils**: For blockchain interaction and ABI handling

## Usage Pattern
1. Initialize app with `InitializeAppTask`
2. Configure events in protocol config files
3. Run `ParentSynchronizeTask` to start event synchronization
4. Monitor sync progress via `last_synced_block` and `blocks_to_sync`
5. Use `ResetAppTask` to clean state when needed

# Oracles Module

## Overview
The oracles module provides sophisticated price oracle management for DeFi protocols, handling complex price calculations for various asset types including liquid staking tokens, stablecoins, and cross-chain assets.

## Models

### `PriceEvent` (models.py:7)
- **Purpose**: Represents price feed events from oracle contracts
- **Inherits from**: `BaseEvent`
- **Key fields**:
  - `asset`: The asset address being priced
  - `asset_source`: The oracle contract address providing price data
  - `asset_source_name`: Human-readable name of the oracle
  - `transmitters`: List of oracle transmitter addresses
- **Unique constraint**: `(asset, asset_source, topic_0)` to prevent duplicates
- **Special methods**: `get_transmitters()` extracts transmitter data from blockchain events

## Tasks

### `InitializePriceEvents` (tasks.py:31)
- **Purpose**: Initialize price event monitoring by discovering asset sources from ClickHouse
- **Operations**:
  - Queries ClickHouse for `AssetSourceUpdated` events
  - Creates `PriceEvent` instances for each asset-source pair
  - Resolves underlying price feed sources
  - Inserts asset source token metadata
- **Caching**: Uses protocol-network-asset-source cache keys for deduplication

### `PriceEventSynchronizeTask` (tasks.py:180)
- **Purpose**: Synchronize price events and extract oracle data
- **Key features**:
  - Groups event logs by contract address
  - Parses numerator and multiplier data from events
  - Bulk inserts into ClickHouse tables (`EventRawNumerator`, `TransactionRawNumerator`, etc.)
  - Updates transmitter information for WebSocket caching
- **Error handling**: Retries failed ClickHouse operations up to 3 times

### `PriceEventDynamicSynchronizeTask` (tasks.py:377)
- **Purpose**: Parent task orchestrating multiple price event synchronization tasks
- **Mixins**: `ParentSynchronizeTaskMixin`
- **Child task**: `PriceEventSynchronizeTask`

### `PriceEventStaticSynchronizeTask` (tasks.py:416)
- **Purpose**: Synchronize static price components (denominator, max cap)
- **Operations**:
  - Processes denominator data for all price events
  - Calculates maximum price caps for price manipulation protection
  - Inserts into `EventRawDenominator` and `EventRawMaxCap` tables

### `PriceTransactionDynamicSynchronizeTask` (tasks.py:442)
- **Purpose**: Synchronize transaction-based multiplier data
- **Operations**: Processes multiplier data for live price calculations

### `VerifyHistoricalPriceTask` (tasks.py:473)
- **Purpose**: Validate historical price data accuracy
- **Verification**: Compares ClickHouse historical prices with live RPC data
- **Threshold**: Uses 1% delta threshold for price validation
- **Logging**: Reports discrepancies for monitoring

## Contract Interfaces

### `PriceOracleInterface` (contracts/interface.py)
- **Purpose**: Core interface for interacting with price oracle contracts
- **Methods**:
  - `latest_price_from_rpc`: Real-time price from blockchain
  - `historical_price_from_event`: Historical price from ClickHouse events
  - `historical_price_from_transaction`: Price from specific transactions

### Price Calculation Components

#### `get_denominator()` (contracts/denominator.py)
- **Purpose**: Calculate denominator values for price normalization
- **Handles**: Price Cap Adapters, Synchronicity Adapters, standard feeds
- **Returns**: Asset info, source type, timestamp, calculated denominator

#### `get_max_cap()` (contracts/max_cap.py)
- **Purpose**: Calculate maximum price caps for manipulation protection
- **Types**:
  - Stable price cap adapters: Extract from blockchain events
  - Dynamic price cap adapters: Calculate from snapshot ratio and growth rate
  - Others: No cap (returns 0)

#### `get_multiplier()` (contracts/multiplier.py)
- **Purpose**: Calculate multiplier values for price adjustments
- **Complex handling for**:
  - Liquid staking tokens (wstETH, rETH, etc.)
  - Pendle discount calculations for time-to-maturity assets
  - Ratio provider types with various methodologies
- **Performance**: Implements caching for historical block data

#### `get_numerator()` (contracts/numerator.py)
- **Purpose**: Extract and process numerator data from price events
- **Special features**:
  - Synchronicity price adapter support
  - Cross-chain price feed handling
  - Advanced caching for price predictions

#### `get_underlying_sources()` (contracts/underlying_sources.py)
- **Purpose**: Recursively resolve underlying price feed dependencies
- **Functionality**: Maps adapter types to their fundamental price sources
- **Use case**: Monitor all price feeds affecting a given asset

### Utilities

#### `RpcCacheStorage` (contracts/utils.py)
- **Purpose**: Caching layer for RPC calls and contract interactions
- **Features**:
  - TTL-based caching
  - Contract info retrieval from Etherscan API
  - Performance optimization for repeated calls

#### `AssetSourceType` (contracts/utils.py)
- **Purpose**: Enum defining all supported oracle contract types
- **Types**: Price Cap Adapters, Synchronicity Adapters, Ratio Providers, etc.

## Materialized Views
Oracle-specific ClickHouse materialized views in `mv_queries/`:
- Raw price event processing (101-107)
- Latest price event aggregation (201-207)
- Price calculation views (301-307)
- Final price computation (401-404)
- Special asset handling (501-507)

## WebSocket Caching
- `cache_transmitters_for_websockets()`: Caches oracle transmitter addresses
- `cache_asset_sources_for_websockets()`: Maps contract addresses to asset sources
- Used for real-time price feed monitoring

## Key Dependencies
- **ClickHouse**: High-performance price data storage and aggregation
- **Web3**: Blockchain interaction and smart contract calls
- **Django Cache**: Redis-based caching for performance
- **Celery**: Asynchronous task processing for price synchronization

## Usage Pattern
1. Initialize with `InitializePriceEvents` to discover oracle contracts
2. Run `PriceEventDynamicSynchronizeTask` for continuous price monitoring
3. Use `PriceEventStaticSynchronizeTask` for static price components
4. Validate with `VerifyHistoricalPriceTask` for data accuracy
5. Access prices via `PriceOracleInterface` for application logic
