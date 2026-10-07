#!/usr/bin/env bash
set -e
docker compose up -d --build
docker compose ps
