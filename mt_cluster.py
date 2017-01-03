"""
Creates a GREASY TASKS FILE

1: greasy task filename to create
2: TF script file
3: number of ps
4: number of gpus to be used
5: 1/0 (All gpus in a worker/One gpu per worker)
6: 1/0 (PSs alongside with workers/ PSs alone)

"""

import sys
from node_utils import get_nodes


def get_file(filename):
    return open(filename, "w")


def get_cluster(num_ps, ps_alongside, num_gpus, all_gpus_in_node):
    init_port = 2220
    nodes = get_nodes()
    if (ps_alongside and num_ps > len(nodes)) or (not ps_alongside and num_ps >= len(nodes)):
        sys.exit("Incorrect PS number")
    ports = [':' + str(x) for x in range(init_port, init_port + num_gpus)]

    if ps_alongside:
        ps_nodes = [n + ':' + str(init_port - 1) for n in nodes[-num_ps:]]
        worker_nodes = nodes
    else:
        ps_nodes = [n + ':' + str(init_port - 1) for n in nodes[:num_ps]]
        index = -1 * (len(nodes) - num_ps)
        worker_nodes = nodes[index:]

    if not all_gpus_in_node:
        worker_nodes_4 = []
        for worker in worker_nodes:
            for port in ports:
                worker_nodes_4.append(worker + port)
        worker_nodes_str = ",".join(worker_nodes_4)
    else:
        worker_nodes_str = ",".join([w + ports[0] for w in worker_nodes])

    ps_nodes_str = ",".join(ps_nodes)

    return {'worker_string': worker_nodes_str, 'ps_string': ps_nodes_str,
            "worker_nodes": worker_nodes, "ps_nodes": ps_nodes}


def get_script_line(name, cluster, script, index, visible_devices):
    return 'CUDA_VISIBLE_DEVICES="' + str(visible_devices) + '" python ' + script + ' --ps_hosts=' + cluster[
        'ps_string'] + ' --worker_hosts=' + \
           cluster['worker_string'] + ' --job_name=' + name + ' --task_index=' + str(index)


def put_in_file(num_ps, ps_alongside, num_gpus, all_gpus_in_node, cluster, script, filename):
    visible_devices = ",".join([str(n) for n in range(num_gpus)])
    if not ps_alongside:
        for ps in range(num_ps):
            line = get_script_line("ps", cluster, script, ps, "")
            print(line, file=filename)
        task_w_index = 0
        for worker in cluster['worker_nodes']:
            lines = []
            if not all_gpus_in_node:  # 4W per node
                for x in range(num_gpus):
                    lines.append(get_script_line("worker", cluster, script, task_w_index, x))
                    task_w_index += 1
            else:  # 1W per node
                lines.append(get_script_line("worker", cluster, script, task_w_index, visible_devices))
                task_w_index += 1
            print(" & ".join(lines), file=filename)
    else:
        task_w_index = 0
        task_ps_index = 0
        worker_index = 0
        num_nodes = len(cluster['worker_nodes'])
        for worker in cluster['worker_nodes']:
            lines = []
            if num_nodes - worker_index <= num_ps:
                if task_ps_index < num_ps:
                    lines.append(get_script_line("ps", cluster, script, task_ps_index, ""))
                    task_ps_index += 1
            worker_index += 1
            if not all_gpus_in_node:  # 4W per node
                for x in range(num_gpus):
                    lines.append(get_script_line("worker", cluster, script, task_w_index, x))
                    task_w_index += 1
            else:  # 1W per node
                lines.append(get_script_line("worker", cluster, script, task_w_index, visible_devices))
                task_w_index += 1
            print(" & ".join(lines), file=filename)


if __name__ == "__main__":
    try:
        filename = sys.argv[1]
        script = sys.argv[2]
        num_ps = int(sys.argv[3])
        num_gpus = int(sys.argv[4])
        all_gpus_in_node = bool(int(sys.argv[5]))
        ps_alongside = bool(int(sys.argv[6]))
    except IndexError:
        sys.exit("Incorrect args")

    file = get_file(filename)
    cluster = get_cluster(num_ps, ps_alongside, num_gpus, all_gpus_in_node)
    put_in_file(num_ps, ps_alongside, num_gpus, all_gpus_in_node, cluster, script, file)
