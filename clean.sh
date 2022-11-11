#! /usr/bin/bash
# Подготовка директории к коммиту файлов

rm -r logs/*
flake8 .
black .
flake8
