import os
import subprocess


def process_nodes(nodes):
    net_suffix = "-ib0"
    return_nodes = []
    node_list_shell = subprocess.check_output("scontrol show hostnames " + nodes, shell=True)
    node_list = str(node_list_shell).split('\\n')[:-1]
    node_list[0] = node_list[0][2:]
    for node in node_list:
        return_nodes.append(node + net_suffix)

    return return_nodes


def get_nodes():
    nodes = os.environ.get('SLURM_JOB_NODELIST')
    if nodes is not None or not '\n':
        return process_nodes(nodes)
    else:
        return []
