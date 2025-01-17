#!/bin/bash

# Add the wofs_ls datasets covering the waterbody UID: edumesbb2
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/204/048/2023/01/*/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/204/049/2023/01/*/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'

s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/205/048/2023/01/*/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/205/049/2023/01/*/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'

s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/206/048/2023/01/*/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/206/049/2023/01/*/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
