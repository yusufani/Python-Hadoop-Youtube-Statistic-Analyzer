# %%

import sys

import docker
from shutil import copyfile
import os
import json
from pymitter import EventEmitter
from datetime import datetime


class DockerEnv:
    def __init__(self):
        self.client = docker.from_env()
        self.status = ""
        self.output = ""
        self.namenode_container = self.client.containers.list(all=True, filters={'name': 'namenode'})[0]
        self.event_emitter = EventEmitter()

    def clear_input(self):
        self.status, self.output = self.namenode_container.exec_run("hadoop fs -rm -r input")
        print(self.output)

    def clear_output(self):

        self.status, self.output = self.namenode_container.exec_run("hadoop fs -rm -r output")
        print(self.output)

    def load_dataset(self, ds_path, para):
        with open("configs.json", "r", encoding="utf-8") as fp:
            configs = json.load(fp)

        ds_name = os.path.basename(para["dataset_path"])
        final_ds_path = os.path.join(configs["hadoop_data_path"], ds_name + "fixed")

        if not os.path.exists(final_ds_path):
            with open(ds_path, "r", encoding="utf-8") as fp:
                print("DEBUG: Could not find ds on path file is being copied")
                rows = fp.readlines()
                with open(final_ds_path, "w", encoding="utf-8") as fp2:
                    fp2.writelines(rows[1:])
                print("DEBUG: file is copied")
        query = "hdfs dfs -put {} input".format(os.path.basename(final_ds_path))

        # print("Output : ", exec_log)
        exec_log = self.namenode_container.exec_run(query, workdir="/map_reducers/",
                                                    stderr=True,
                                                    stdout=True,
                                                    stream=True)
        self.update_status_label("Uploading dataset...")
        for line in exec_log[1]:
            print(line)

        print("DEBUG: Dataset Uploaded")
        self.update_status_label("Finished uploading dataset...")

    def count_minMax_mean_wordCount(self, para):
        mapper_query = '-mapper "mapper.py {} {} "'.format(para["key"], para["value"])
        reducer_query = '-reducer reducer.py '
        file = "-file {}/mapper.py -file {}/reducer.py".format("DS_" + para["function"],
                                                               "DS_" + para["function"])
        query = "hadoop jar hadoop-streaming-3.2.1.jar  {} {} -input input -output output {}".format(
            mapper_query,
            reducer_query,
            file)
        print("DEBUG RUNNING QUERY : ", query)
        exec_log = self.namenode_container.exec_run(query, workdir="/map_reducers/",
                                                    stderr=True,
                                                    stdout=True,
                                                    stream=True)
        for line in exec_log[1]:
            print(line)
            self.update_map_reduce_progress_bar(line)

        print("!COMPLETED!")
        self.status, self.output = self.namenode_container.exec_run("hdfs dfs -cat output/part-00000")
        print(self.output)
        return self.output

    def averageif(self, para):
        mapper_query = '-mapper "mapper.py {} {} {} {} {} "'.format(para["key"], para["value"],
                                                                    para["extra_options"]["key2"],
                                                                    para["extra_options"]["operator"],
                                                                    para["extra_options"]["value2"])
        reducer_query = '-reducer "reducer.py "'
        file = "-file {}/mapper.py -file {}/reducer.py".format("DS_" + para["function"],
                                                               "DS_" + para["function"])
        query = "hadoop jar hadoop-streaming-3.2.1.jar  {} {} -input input -output output {}".format(
            mapper_query,
            reducer_query,
            file)
        print("DEBUG RUNNING QUERY : ", query)
        exec_log = self.namenode_container.exec_run(query, workdir="/map_reducers/",
                                                    stderr=True,
                                                    stdout=True,
                                                    stream=True)
        for line in exec_log[1]:
            print(line)
            self.update_map_reduce_progress_bar(line)

        print("!COMPLETED!")
        self.status, self.output = self.namenode_container.exec_run("hdfs dfs -cat output/part-00000")
        print(self.output)
        return self.output

    def large(self, para):
        mapper_query = '-mapper "mapper.py {} {} "'.format(para["key"], para["value"])
        reducer_query = '-reducer "reducer.py {}"'.format(para["extra_options"]["k"])
        file = "-file {}/mapper.py -file {}/reducer.py".format("DS_" + para["function"],
                                                               "DS_" + para["function"])
        query = "hadoop jar hadoop-streaming-3.2.1.jar  {} {} -input input -output output {}".format(
            mapper_query,
            reducer_query,
            file)
        print("DEBUG RUNNING QUERY : ", query)
        exec_log = self.namenode_container.exec_run(query, workdir="/map_reducers/",
                                                    stderr=True,
                                                    stdout=True,
                                                    stream=True)
        for line in exec_log[1]:
            print(line)
            self.update_map_reduce_progress_bar(line)

        print("!COMPLETED!")
        self.status, self.output = self.namenode_container.exec_run("hdfs dfs -cat output/part-00000")
        print(self.output)
        return self.output

    def update_map_reduce_progress_bar(self, line):
        linestr = line.decode("ascii")
        if "mapreduce.Job:  map" in linestr:
            progress_str = linestr.split("map ")[1]
            map_progress = progress_str.split("%")[0]
            reduce_progress = linestr.split("reduce ")[1]
            reduce_progress = reduce_progress.split("%")[0]
            print("MAP_PROGRESS: ", map_progress)
            print("REDUCE_PROGRESS: ", reduce_progress)

            map_progress = int(map_progress)
            reduce_progress = int(reduce_progress)

            map_reduce_progress = (map_progress + reduce_progress) / 2
            self.event_emitter.emit("OnMapReduceProgressChange", {"line": linestr, "progress": map_reduce_progress})
        else:
            self.event_emitter.emit("OnMapReduceProgressChange", {"line": linestr})

    def update_status_label(self, newStatus):
        self.event_emitter.emit("OnStatusChange", newStatus)

    def run(self, ds_path, para):
        try:
            self.update_status_label("Started hadoop job function {}...".format(para["function"]))
            self.event_emitter.emit("OnMapReduceProgressChange",
                                    {
                                        "line": "\n" + 50 * "*" + "\n" + 50 * "*" + "\nSTARTING PROCESS\n" + 50 * "*" + "\n" + 50 * "*",
                                        "progress": 0})

            self.clear_input()
            start_time = datetime.now()
            self.load_dataset(ds_path, para)
            passed_time = str((datetime.now() - start_time).total_seconds())
            self.event_emitter.emit("OnMapReduceProgressChange",
                                    {"line": "\n-----PASSED TIME for loading dataset:" + passed_time+"-----\n"})
            self.clear_output()
            self.update_status_label("Running hadoop job function {}...".format(para["function"]))
            start_time = datetime.now()
            if para["function"] == "AVERAGEIF":
                data = self.averageif(para)
            elif para["function"] in ["COUNT", "MEAN", "MIN-MAX", "WORD-COUNT"]:
                data = self.count_minMax_mean_wordCount(para)
            elif para["function"] == "LARGE":
                data = self.large(para)
            else:
                raise NotImplementedError
            passed_time = str((datetime.now() - start_time).total_seconds())
            self.event_emitter.emit("OnMapReduceProgressChange",
                                    {"line": "\n-----PASSED TIME for loading dataset:" + passed_time+"-----\n"})
            return self.fix_format(data)

        except Exception as e:
            print(e)

    def fix_format(self, text):
        text = text.decode("utf-8")
        text = "\n"+ 50 * "#" + "\n" + text + 50 * "#" + "\n"
        self.event_emitter.emit("OnMapReduceProgressChange", {"line": text})
        return text
