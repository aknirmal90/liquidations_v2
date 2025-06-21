# Aave Asset Dashboard

A beautiful and modern web dashboard for exploring Aave protocol assets with detailed configuration information and visualizations.

## Features

- **Asset List View**: Browse all assets in the Aave protocol with key metrics in a single-column layout
- **Asset Detail View**: Detailed information for each asset including:
  - Configuration parameters (LTV, Liquidation Threshold, Liquidation Bonus)
  - E-Mode settings
  - Token metadata
  - Interactive Bokeh visualizations
- **Search Functionality**: Filter assets by name or symbol
- **Responsive Design**: Modern UI that works on desktop and mobile
- **Real-time Data**: Connected to ClickHouse database for live data
- **Authentication Required**: All views are protected behind login

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
