# Aave Asset Dashboard

A comprehensive dashboard for monitoring Aave protocol assets, including detailed configuration information and real-time price data.

## Features

### Overview Tab
- **Asset Configuration Details**: Complete view of LTV, liquidation thresholds, and bonuses for both collateral and E-Mode
- **Token Information**: Name, symbol, decimals, and protocol details
- **Token Addresses**: All associated contract addresses (asset, aToken, debt tokens, interest rate strategy)
- **Configuration Charts**: Visual comparison of collateral vs E-Mode parameters
- **Historical Events**: Timeline of asset-related events

### Prices Tab (New!)
- **Current Price**: Real-time price from `aave_ethereum.LatestRawPriceEvent`
- **Asset Source**: Price oracle source information
- **Average Block Time**: Calculated from the last 1000 price records
- **Price History Chart**: Interactive timeseries visualization of historical prices
- **Price Distribution**: Histogram showing price distribution patterns
- **Asset Source Updates**: Table of all `AssetSourceUpdated` events for the asset

## Data Sources

### Price Data
- **Current Price**: `aave_ethereum.LatestRawPriceEvent`
- **Historical Prices**: `aave_ethereum.RawPriceEvent` (last 1000 records)
- **Asset Source Events**: `aave_ethereum.AssetSourceUpdated`

### Asset Configuration
- **Asset Details**: `aave_ethereum.view_LatestAssetConfiguration`

## Technical Implementation

### Backend (Django Views)
- **Enhanced `asset_detail` view**: Fetches comprehensive price and configuration data
- **Price Calculations**: Average block time calculation from historical data
- **Data Validation**: Robust error handling for price data processing
- **Bokeh Visualizations**: Interactive charts for price history and distribution

### Frontend (Bootstrap + Custom CSS)
- **Tabbed Interface**: Clean separation between Overview and Prices
- **Responsive Design**: Mobile-friendly layout
- **Interactive Charts**: Hover tools and zoom capabilities
- **Modern UI**: Gradient backgrounds and glass-morphism effects

### Key Functions
- `calculate_average_block_time()`: Analyzes time differences between consecutive price updates
- `create_price_plots()`: Generates Bokeh visualizations for price data
- `create_asset_plots()`: Creates configuration comparison charts

## Usage

1. Navigate to the asset list page
2. Click on any asset to view its details
3. Use the tabs to switch between Overview and Prices views
4. Interact with charts using hover, zoom, and pan tools
5. View detailed price metrics and asset source information

## Dependencies

- Django 4.x
- Bokeh (for interactive charts)
- NumPy (for statistical calculations)
- Bootstrap 5.3.0
- Font Awesome 6.4.0

## Data Quality Features

- **Price Validation**: Only positive, non-null prices are included
- **Timestamp Processing**: Robust handling of various timestamp formats
- **Error Handling**: Graceful degradation when data is unavailable
- **Performance**: Optimized queries with appropriate limits

## Future Enhancements

- Price alerts and notifications
- Comparative analysis between assets
- Export functionality for price data
- Real-time price updates via WebSocket
- Advanced statistical analysis (volatility, correlation)

## URLs

- `/dashboard/` - Asset list page (requires login)
- `/dashboard/asset/<asset_address>/` - Individual asset detail page (requires login)
- `/dashboard/api/asset/<asset_address>/` - API endpoint for asset data (requires login)

## Authentication

All dashboard views require authentication. Users will be redirected to the Django admin login page if not authenticated.

- **Login URL**: `/admin/login/`
- **Logout URL**: `/admin/logout/`
- **Redirect after login**: `/dashboard/`

## Setup

1. The dashboard app is already added to `INSTALLED_APPS` in Django settings
2. URLs are configured in the main project's `urls.py`
3. Templates are located in `dashboard/templates/dashboard/`
4. Authentication settings are configured in `settings_generic.py`

## Dependencies

- Django 5.1.2+
- ClickHouse Connect (for database queries)
- Bokeh (for interactive visualizations)
- Bootstrap 5.3.0 (for UI components)
- Font Awesome 6.4.0 (for icons)

## Bokeh Integration

Interactive charts and visualizations are now enabled:

- **Configuration Parameters Chart**: Bar chart comparing Collateral vs E-Mode settings
- **Historical Events Chart**: Line chart showing event frequency over time
- **Interactive Features**: Zoom, pan, reset, and save functionality

## Data Source

The dashboard queries the `aave_ethereum.view_LatestAssetConfiguration` view in ClickHouse, which provides:

- Asset addresses and metadata
- Collateral configuration parameters
- E-Mode settings
- Interest rate strategy addresses

## Customization

The dashboard uses a modern design system with:

- CSS custom properties for consistent theming
- Glassmorphism effects with backdrop blur
- Smooth animations and transitions
- Responsive grid layout
- Single-column asset list layout

You can customize the appearance by modifying the CSS in the template files.
