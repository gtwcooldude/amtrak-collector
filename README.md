# Amtrak Real-Time Data Collector 🚄

24/7 data collection service that fetches Amtrak train positions and stores them to PostgreSQL for historical analysis and forecasting.

## What it does

- Fetches train positions every **30 seconds**
- Calculates KPIs every **5 minutes**
- Stores everything to PostgreSQL
- Builds patterns for forecasting
- Provides health check endpoint

## Deployment

This is designed for Railway.app deployment:

1. Connect this repo to Railway
2. Add PostgreSQL database
3. Link the `DATABASE_URL` variable
4. Deploy!

## Environment Variables

- `DATABASE_URL` - PostgreSQL connection string (auto-set by Railway)
- `PORT` - Server port (auto-set by Railway)

## Endpoints

- `/` - Service info
- `/health` - Health check with latest KPIs
- `/stats` - Database statistics

## Data Collected

- **vehicle_positions** - Train locations, speeds, status
- **trip_updates** - Delays and schedule changes
- **kpi_snapshots** - Calculated metrics every 5 min
- **hourly_patterns** - For forecasting
