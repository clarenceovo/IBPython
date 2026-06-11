"""Alerts router — consolidated into system.py.

All endpoints previously defined here have been migrated:
- POST /system/market-data-type  → system.router
- GET  /system/server-time       → system.router
- GET  /market-data/depth/exchanges → system._market_data_router
"""
