import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 18})
import os
import numpy as np

plt.style.use('dark_background')

import datetime as d


def clear_info_lines(data):
    return [line for line in data.split("\n")[1:-1] if "INFO:" not in line]


MAX_KEY_LIMIT = 30 # Important Limit


def make_graph(data):
    data = clear_info_lines(data)
    print("CLEARED DATA", data)
    paths = []

    x = np.arange(len(data)) if len(data) <= MAX_KEY_LIMIT else np.arange(MAX_KEY_LIMIT)  # the label locations
    n_val = len(data[0].split("\t")) - 1
    os.makedirs("graphs", exist_ok=True)

    for i in range(n_val):
        labels = []
        fig, ax = plt.subplots(figsize=(40, 10), dpi=100)
        for idx, info in enumerate(data):
            info = info.split("\t")
            key = info[0]
            labels.append(key)
            value = info[i + 1]
            width = 0.35
            cordinat = x[idx]
            try:
                value = float(value)
            except Exception as e:
                print("Error while converting to float ", e)

            rects1 = ax.bar([cordinat], [value], width)
            ax.bar_label(rects1, padding=3)

        # Add some text for labels, title and custom x-axis tick labels, etc.
        ax.set_xlabel('Keys')
        ax.set_ylabel('Values')
        ax.set_title('Output Analysis')
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        # ax.legend()
        fig.tight_layout()
        file_name = d.datetime.now().isoformat().replace(":", "_") + ".jpg"
        path = os.path.join("graphs", file_name)
        paths.append(path)
        plt.savefig(path)
    return paths


"""
make_graph(data)
"""
