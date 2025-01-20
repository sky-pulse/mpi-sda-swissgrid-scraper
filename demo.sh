#!/usr/bin/env bash

python swissgrid_scraper.py \
    --log-level="DEBUG" \
    --case-study-name "climate" \
    --tracer-id "swissgrid" --job-id "1" \
    --kp_auth_token test123 --kp_host localhost --kp_port 8000 --kp_scheme http 
