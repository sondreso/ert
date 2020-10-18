#!/bin/env python
from collections import defaultdict
import matplotlib.pyplot as plt
import json
import sys

def main(parameters_path, num_realizations, num_iterations):
    averages = {}
    for i in range(num_iterations):
        iter_mean = defaultdict(int)
        for r in range(num_realizations):
            with open(parameters_path.format(r, i)) as fh:
                data = json.load(fh)["RESTRICTIONS"]
            for k, v in data.items():
                iter_mean[int(k[4:])] += v
        mean_vector = [None]*len(iter_mean.keys())
        for k in iter_mean.keys():
            mean_vector[k] = iter_mean[k] / num_realizations
        averages[i] = mean_vector
        plt.subplot(221+i)
        plt.plot(averages[i],label='Iter {}'.format(i))
        plt.vlines(x=35, ymin=0, ymax=1, colors='r', label="12. Mars")
        axes = plt.gca()
        axes.set_ylim([0,1])
        plt.legend()
        plt.ylabel('Avg. Modifier')
        plt.xlabel('Days')
    plt.show()

if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
