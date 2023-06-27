#!/usr/bin/env python3

import csv
from os import path
from glob import glob
from pathlib import Path
from decimal import Decimal

lookup = {}
entity = {}
datasets = {}

csv.field_size_limit(1000000000)

# ignoring resource for now ..
def key(row):
    return "|".join([row.get(f, "") for f in ["prefix", "value"]])


for row in csv.DictReader(open("specification/dataset.csv")):
    entity[row["dataset"]] = Decimal(row.get("entity-minimum", "") or "0")
    datasets[row["dataset"]] = row


if path.exists("pipeline/lookup.csv"):
    for row in csv.DictReader(open("pipeline/lookup.csv")):
        lookup[key(row)] = row
        e = row.get("entity", "")
        if e:
            e = Decimal(row["entity"])
            dataset = row["prefix"]
            if e > entity[dataset]:
                entity[dataset] = e + 1


for directory in glob("issue/*"):
    dataset = Path(directory).name
    for path in glob(directory + "/*.csv"):
        print(path)
        for row in csv.DictReader(open(path)):
            if row["issue-type"] == "unknown entity":
                (prefix, reference) = row["value"].split(":", 1)
                row["prefix"] = prefix
                row["reference"] = reference
                row["value"] = reference
                row["resource"] = ""
                row["entry-number"] = ""
                lookup[key(row)] = row


w = csv.DictWriter(open("pipeline/lookup.csv", "w", newline=""), ["prefix", "resource", "entry-number", "value", "entity"], extrasaction="ignore")
w.writeheader()

for key, row in lookup.items():
    if not row["value"]:
        continue

    if not  row.get("entity", ""):
        prefix = row["prefix"]
        row["entity"] = entity[prefix]
        entity[prefix] = entity[prefix] + 1
    w.writerow(row)
