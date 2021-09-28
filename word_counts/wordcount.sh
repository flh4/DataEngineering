#!/bin/bash

cat ${1} | tr -cs A-Za-z '\n' | tr A-Z a-z | sort | uniq -c | sort -rn | sed ${2}q
