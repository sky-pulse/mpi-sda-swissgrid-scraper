#!/usr/bin/env bash

python swissgrid_scraper.py \
    --log-level="DEBUG" \
    --case-study-name "Swissgrid" \
    --start_date "2023-01-01T00:00"\
    --end_date "2023-12-31T00:00"\
    --latitude 47.600968892668575 \
    --longitude 8.183936440913271 \
    --data_type "thermal" \
    --sentinel_client_id "YOUR SENTINEL HUB CLIENT ID" \
    --sentinel_client_secret "YOUR SENTINEL HUB CLIENT SECRET"\
    --tracer-id "swissgrid" --job-id "1" \
    --kp_auth_token test123 --kp_host localhost --kp_port 8000 --kp_scheme http 
