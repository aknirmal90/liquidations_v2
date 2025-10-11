# MEV Share Implementation Guide

This implementation adds support for monitoring and analyzing MEV (Maximal Extractable Value) opportunities using Flashbots MEV Share streaming data.

## Overview

The MEV Share implementation consists of:

1. **Management Command** (`listen_mev_share.py`) - Connects to Flashbots MEV Share streaming API
2. **Data Model** (`MevShareTransactionLog`) - Stores MEV transaction data for analysis
3. **Processing Task** (`ProcessMevShareTransactionTask`) - Analyzes MEV opportunities
4. **Admin Interface** - View and analyze MEV transactions in Django admin

## Features

- Real-time monitoring of pending transactions via Flashbots MEV Share websocket
- Filters for specific topic0 hash: `0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f`
- MEV opportunity detection and analysis
- Integration with existing price monitoring infrastructure
- Admin interface for data visualization and analysis

## Usage

### Running the MEV Share Listener

```bash
# Basic usage - connects to default MEV Share endpoint
python manage.py listen_mev_share --network ethereum --provider mev-share

# With custom MEV Share endpoint
python manage.py listen_mev_share --network ethereum --provider mev-share --mev-share-endpoint wss://custom-mev-endpoint.com

# For testing with other networks (if MEV Share supports them)
python manage.py listen_mev_share --network polygon --provider mev-share
```

### Command Arguments

- `--network`: Network name (must exist in your `blockchains.Network` model)
- `--provider`: Provider identifier (used for data source tracking)
- `--mev-share-endpoint`: Custom MEV Share websocket endpoint (default: `wss://mev-share.flashbots.net`)

## Database Schema

### MevShareTransactionLog Model

```python
class MevShareTransactionLog(models.Model):
    # Transaction identification  
    transaction_hash = models.CharField(max_length=66, unique=True)
    asset_address = models.CharField(max_length=42)
    network = models.ForeignKey("blockchains.Network", on_delete=models.PROTECT)
    
    # Price data
    price = models.DecimalField(max_digits=72, decimal_places=0, null=True, blank=True)
    round_id = models.PositiveIntegerField(null=True, blank=True)
    block_height = models.PositiveIntegerField(null=True, blank=True)
    
    # Timestamps
    mev_received_at = models.DateTimeField()  # When received from MEV Share
    onchain_created_at = models.DateTimeField(null=True, blank=True)  # Transaction timestamp
    processed_at = models.DateTimeField()  # When processing completed
    db_created_at = models.DateTimeField(auto_now_add=True)
    
    # MEV Analysis fields
    is_mev_opportunity = models.BooleanField(default=False)
    frontrun_detected = models.BooleanField(default=False)
    backrun_detected = models.BooleanField(default=False)
    
    # Raw data for debugging
    raw_transaction_data = models.JSONField(null=True, blank=True)
```

## MEV Analysis Features

The implementation includes basic MEV opportunity detection:

1. **Time-based Analysis**: Looks for related transactions within a 10-second window
2. **Price Anomaly Detection**: Compares prices to recent averages to detect significant changes (>1%)
3. **Transaction Clustering**: Groups related MEV activities

## Data Flow

1. **MEV Share Websocket** receives pending transaction data
2. **Transaction Parser** extracts relevant log data matching the target topic0
3. **Price Cache Check** verifies if this represents a new price update
4. **Dual Processing**:
   - `UpdateAssetPriceTask` - Updates regular price tracking
   - `ProcessMevShareTransactionTask` - Stores MEV-specific data and analysis
5. **MEV Analysis** - Detects potential MEV opportunities
6. **Database Storage** - Saves data for analysis and visualization

## Monitoring and Analysis

### Django Admin Interface

Access the MEV Share data through Django admin:

- **URL**: `/admin/aave/mevsharetransactionlog/`
- **Features**:
  - Filter by MEV opportunity, network, timestamps
  - Search by transaction hash or asset address
  - View transaction details with explorer links
  - Analyze timing delays and processing performance
  - Examine raw transaction data

### Key Metrics Available

- **MEV Detection Delay**: Time between on-chain creation and MEV Share detection
- **Processing Delay**: Time between MEV detection and processing completion
- **MEV Opportunity Rate**: Percentage of transactions flagged as MEV opportunities
- **Price Impact Analysis**: Comparison with recent price averages

## Integration with Existing System

The MEV Share implementation integrates seamlessly with your existing infrastructure:

- Uses the same `WebsocketCommand` base class as `listen_pending_transactions.py`
- Leverages existing `UpdateAssetPriceTask` for price updates
- Stores data in the same database with related models
- Uses the same cache infrastructure for price deduplication

## Configuration

### Environment Variables

No additional environment variables required. Uses existing Django settings.

### Network Configuration

Ensure your target network exists in the `blockchains.Network` model with:
- Correct `name` field
- Valid RPC endpoints (for fallback data if needed)

### Asset Configuration

The command automatically uses assets from your `aave.Asset` model that match the network.

## Troubleshooting

### Common Issues

1. **Connection Issues**:
   - Verify MEV Share endpoint is accessible
   - Check network connectivity
   - Review websocket connection logs

2. **No Data Received**:
   - Confirm target topic0 hash is correct
   - Verify asset addresses are in your database
   - Check MEV Share subscription format

3. **Processing Errors**:
   - Monitor Celery task queue for processing tasks
   - Check database connectivity
   - Review error logs for parsing issues

### Debugging

Enable debug logging to see detailed message processing:

```python
# In Django settings
LOGGING = {
    'loggers': {
        'aave.management.commands.listen_mev_share': {
            'level': 'DEBUG',
        },
    }
}
```

## Performance Considerations

- **Websocket Connection**: Auto-reconnects on failure with exponential backoff
- **Message Processing**: Asynchronous task processing to avoid blocking
- **Data Storage**: Efficient indexing on commonly queried fields
- **Memory Usage**: Minimal caching, relies on database for persistence

## Future Enhancements

Potential improvements to consider:

1. **Advanced MEV Detection**: Machine learning models for better opportunity detection
2. **Real-time Alerts**: Notification system for high-value MEV opportunities  
3. **Arbitrage Analysis**: Calculate potential profit from detected opportunities
4. **Cross-Chain Support**: Extend to other networks as MEV Share expands
5. **Historical Analysis**: Batch processing of historical MEV data
6. **API Integration**: RESTful API for external MEV analysis tools

## Security Considerations

- **Data Validation**: All incoming data is validated before processing
- **Rate Limiting**: Built-in reconnection delays prevent API abuse
- **Access Control**: Admin interface requires appropriate permissions
- **Data Privacy**: Raw transaction data stored securely with proper indexing

## Deployment Checklist

Before deploying to production:

- [ ] Run database migrations: `python manage.py migrate`
- [ ] Configure Celery workers for task processing
- [ ] Set up monitoring for websocket connection health
- [ ] Configure log rotation for MEV Share logs
- [ ] Test admin interface access and permissions
- [ ] Verify MEV Share endpoint accessibility from production environment
- [ ] Set up backup procedures for MEV transaction data