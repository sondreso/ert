#!/bin/env python
import json
import sys

restriction_template = """
[restriction day-{day}]
day-begins = {day}
day-ends = {day}
infectivity modifier = {modifier}

"""


def main():
    days = 300

    with open("parameters.json") as fh:
        data = json.load(fh)["RESTRICTIONS"]


    with open(f"{sys.argv[1]}") as fh:
        config_content = fh.read()

    with open("seir-config.ini", "w") as fh:
        fh.write(config_content)
        for d in range(days):
            fh.write(restriction_template.format(day=d, modifier=data["DAY_{}".format(d)]))
