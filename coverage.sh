#!/usr/bin/env bash

coverage run --concurrency=multiprocessing -m unittest discover tests
coverage combine
coverage report -m --include=./mystore/*
