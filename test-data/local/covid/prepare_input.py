#!/bin/env python
import json
import sys

restriction_template = """
[restriction day-{day}]
day-begins = {day}
day-ends = {endday}
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
            fh.write(restriction_template.format(day=d, endday=d+1, modifier=data["DAY_{}".format(d)]))


if __name__ == "__main__":
    main()