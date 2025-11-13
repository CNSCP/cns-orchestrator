#!/bin/bash

#
# Startup script for Docker container
#
# ADuss @mindovermiles262
# 2020-11-08
#

if [ "$NODE_ENV" = "production" ];
then
  echo "[*] Running production server"
  npm run production
else
  echo "[*] Running staging server"
  npm run staging
fi
