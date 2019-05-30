#!/usr/bin/env bash

coverage run --concurrency=multiprocessing tests.py
coverage combine
coverage report -m --include=./mystore/*
