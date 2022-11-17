#! /usr/bin/bash
# Подготовка директории к коммиту файлов

rm -r logs/*
rm *~
flake8 .
black .
flake8
