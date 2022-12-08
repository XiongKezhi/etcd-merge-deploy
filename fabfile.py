import concurrent.futures
import os
import sys
import time
import traceback

import requests
from fabric import task, Connection
from invoke import UnexpectedExit


# ------------------DEPLOYMENT-------------------------
def extract_clusters(ctx):
    return ctx.config['clusters']


@task
def config(ctx):
    """
    Print clusters' configuration.
    """
    for label, clr in extract_clusters(ctx).items():
        print('{}: {}'.format(label, clr))


@task
def upload(ctx):
    """
    Upload etcd code to servers.
    """
    node = list(extract_clusters(ctx).values())[0][0]
    with Connection(host=node['ip'], connect_kwargs={'password': ctx.config['password']}) as conn:
        conn.run('rm -rf ' + ctx.config['root'])
        conn.run('mkdir ' + ctx.config['root'])
    os.system('scp -r ../etcd vdi-linux-030.khoury.northeastern.edu:'+ctx.config['root'])

@task
def clean(ctx):
    """
    Clean all files for recovery, including WAL and snapshots.
    """
    node = list(extract_clusters(ctx).values())[0][0]
    with Connection(host=node['ip'], connect_kwargs={'password': ctx.config['password']}) as conn:
        with conn.cd(ctx.config['dir']):
            conn.run('rm -rf raftexample-*')


@task
def build(ctx):
    node = list(extract_clusters(ctx).values())[0][0]
    with Connection(host=node['ip'], connect_kwargs={'password': ctx.config['password']}) as conn:
        with conn.cd(ctx.config['dir']):
            conn.run('rm -rf raftexample')
            conn.run('go build -o raftexample')


def start_cmd(ctx, nid, ip, cluster_param):
    print('start node #{} on host {}'.format(nid, ip))
    with Connection(host=ip, connect_kwargs={'password': ctx.config['password']}) as conn:
        with conn.cd(ctx.config['dir']):
            cmd = './raftexample --id {} --cluster {} --port {} --mergeport {}'.format(
                nid, cluster_param, ctx.config['httpPort'], ctx.config['mergePort'], nid)
            if 'pullBatchSize' in ctx.config:
                cmd += ' --pullBatchSize ' + str(ctx.config['pullBatchSize'])
            conn.run('nohup {} > raftexample-{}.out 2>&1 &'.format(cmd, nid), asynchronous=True)


@task
def start_node(ctx, id):
    """
    Start node by id
    """
    for clr in extract_clusters(ctx).values():
        target = None
        cluster_param = []
        for node in clr:
            cluster_param.append('{}=http://{}:{}'.format(node['id'], node['ip'], ctx.config['raftPort']))
            if node['id'] == int(id):
                target = node
        if target is not None:
            start_cmd(ctx, target['id'], target['ip'], ','.join(cluster_param))
            return


@task
def start_cluster(ctx, label):
    """
    Start cluster of given label
    """
    cluster = extract_clusters(ctx)[label]
    cluster_param = []
    for node in cluster:
        cluster_param.append('{}=http://{}:{}'.format(node['id'], node['ip'], ctx.config['raftPort']))

    print('start cluster ' + label)
    for node in cluster:
        start_cmd(ctx, node['id'], node['ip'], ','.join(cluster_param))


@task
def start(ctx):
    """
    Start all nodes
    """
    for label, clr in extract_clusters(ctx).items():
        start_cluster(ctx, label)


def stop_cmd(ctx, ip):
    with Connection(host=ip, connect_kwargs={'password': ctx.config['password']}) as conn:
        try:
            conn.run('kill -9 $(/usr/sbin/lsof -t -i:{})'.format(ctx.config['raftPort']), hide='both')
            print('stop running instance on node {}'.format(ip))
        except UnexpectedExit:
            print('no running instance to stop on node {}'.format(ip))


@task
def stop_node(ctx, id):
    """
    Stop node by id
    """
    for clr in extract_clusters(ctx).values():
        for node in clr:
            if node['id'] == int(id):
                stop_cmd(ctx, node['ip'])
                return


@task
def stop_cluster(ctx, label):
    """
    stop cluster of given label
    """
    print('stop cluster ' + label)
    for node in extract_clusters(ctx)[label]:
        stop_cmd(ctx, node['ip'])


@task
def stop(ctx):
    """
    Stop all nodes
    """
    for label, clr in extract_clusters(ctx).items():
        print('stop ' + label)
        for node in clr:
            stop_cmd(ctx, node['ip'])


# ------------------DEPLOYMENT-------------------------


# ------------------TESTS SUITE-------------------------
def test_suite(ctx, clusters, test_func, clean_after=True):
    """
    Create clean test environment by cleaning up given clusters before and after testing.
    """
    print('----------BEFORE TEST: CLEAN ENVIRONMENT----------')
    for clr in clusters:
        stop_cluster(ctx, clr)
    print('clean running files')
    clean(ctx)
    print('--------------------------------------------------')
    print()

    passed = False
    try:
        build(ctx)
        test_func(ctx)
        passed = True
    except:
        ex_type, ex_message, ex_traceback = sys.exc_info()
        print("Exception happens during test:\nTraceback (most recent call last):")
        for trace in traceback.extract_tb(ex_traceback):
            print("  File \"{}\", line {}, in {}\n    {}".format(trace[0], trace[1], trace[2], trace[3]))
        print('{}: {}'.format(ex_type.__name__, ex_message))
        raise
    finally:
        if passed:
            print('\nTEST PASSED!')
        else:
            print('\nTEST FAILED!')

        if clean_after:
            print()
            print('----------AFTER TEST: CLEAN ENVIRONMENT----------')
            for clr in clusters:
                stop_cluster(ctx, clr)
            print('clean running files')
            clean(ctx)
            print('--------------------------------------------------')

        return passed


def assert_that(condition, message=''):
    assert condition, message
# ------------------ TESTS SUITE -------------------------


# ------------------INTEGRATION TESTS-------------------------
def lookup_ip(ctx, nid):
    """
    Look up node ip by id
    """
    for clr in extract_clusters(ctx).values():
        for node in clr:
            if node['id'] == int(nid):
                return node['ip']


def merge_json(ctx, nodes):
    cluster = {}
    for node in nodes:
        cluster[node['id']] = {
            'ip': node['ip'],
            'raftPort': ctx.config['raftPort'],
            'mergePort': ctx.config['mergePort'],
        }
    json = {'clusters': [cluster]}
    return json


def put(ctx, nid, kvs, wait=True):
    ip = lookup_ip(ctx, nid)
    s = requests.session()
    for k, v in kvs.items():
        if wait:
            s.put('http://{}:{}/wait/{}'.format(ip, ctx.config['httpPort'], k), data=v.encode())
        else:
            s.put('http://{}:{}/{}'.format(ip, ctx.config['httpPort'], k), data=v.encode())


def get(ctx, nid, key):
    return requests.get('http://{}:{}/{}'.format(lookup_ip(ctx, nid), ctx.config['httpPort'], key))


@task
def test_all(ctx):
    tests = [test_merge_no_failure,
             test_merge_empty_no_failure,
             test_merge_concurrent,
             test_merge_single_coordinator_failure,
             test_merge_single_candidate_failure]

    passed = 0
    for idx, t in enumerate(tests):
        print('******************** TEST #{} *******************'.format(idx + 1))
        passed += t(ctx)
        print('*************************************************\n\n')
    print('TESTS PASSED: {}/{}'.format(passed, len(tests)))


@task
def test_merge_no_failure(ctx):
    """
    Test merge clusters under no failures.
    Merge coordinator cluster of nodes 1,2,3 and cluster of nodes 4,5,6.
    """

    def __test__(ctx):
        clusters = extract_clusters(ctx)
        cluster1_nodes = [node for node in clusters['integration-test-1']]
        cluster2_nodes = [node for node in clusters['integration-test-2']]

        # start cluster 1 and prepare some data
        start_cluster(ctx, 'integration-test-1')
        print()
        print('prepare some data on cluster integration-test-1')
        print()
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(0, 100)})
        print('check prepared data on cluster integration-test-1')
        for node in cluster1_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(0, 100):
                got = get(ctx, cluster1_nodes[0]['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))
        print()

        # start cluster 2 and prepare some data
        start_cluster(ctx, 'integration-test-2')
        print()
        print('prepare some data on cluster integration-test-2')
        put(ctx, cluster2_nodes[0]['id'], {x: str(x) for x in range(100, 200)})
        print('check prepared data on cluster integration-test-2')
        for node in cluster2_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(100, 200):
                got = get(ctx, cluster2_nodes[0]['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))
        print()

        # merge clusters
        print('merge clusters')
        resp = requests.post('http://{}:{}/merge'.format(lookup_ip(ctx, 1), ctx.config['httpPort']),
                             json=merge_json(ctx, cluster2_nodes))
        assert_that(resp.status_code == 200)
        mid = resp.json()['id']
        while resp.text.lower() != 'completed':
            time.sleep(1)
            resp = requests.get('http://{}:{}/merge/{}'.format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], mid))
        print('merge completed')
        print()

        # check prepared data
        print('check all prepared data on all nodes')
        for node in cluster1_nodes + cluster2_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(0, 200):
                got = get(ctx, node['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))
        print()

        # prepare more data
        print('prepare more data')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(200, 300)})
        print('check prepared data on all nodes')
        for node in cluster1_nodes + cluster2_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(0, 300):
                got = get(ctx, node['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))
        print()

        # restart all nodes with merged cluster configuration
        print('restart nodes with merged cluster configuration')
        stop_cluster(ctx, 'integration-test-1')
        stop_cluster(ctx, 'integration-test-2')
        cluster_param_list = []
        for node in cluster1_nodes + cluster2_nodes:
            cluster_param_list.append('{}=http://{}:{}'.format(node['id'], node['ip'], ctx.config['raftPort']))
        cluster_param = ','.join(cluster_param_list)
        for node in cluster1_nodes + cluster2_nodes:
            start_cmd(ctx, node['id'], node['ip'], cluster_param)
        print()

        # prepare more data
        print('prepare more data')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(300, 400)})
        print('check all prepared data on all nodes')
        for node in cluster1_nodes + cluster2_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(0, 400):
                got = get(ctx, node['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))
        print()

    return test_suite(ctx, ['integration-test-1', 'integration-test-2'], __test__)


@task
def test_merge_empty_no_failure(ctx):
    """
    Test merge clusters without any data yet under no failures.
    Merge coordinator cluster of nodes 1,2,3 and cluster of nodes 4,5,6.
    """

    def __test__(ctx):
        clusters = extract_clusters(ctx)
        cluster1_nodes = [node for node in clusters['integration-test-1']]
        cluster2_nodes = [node for node in clusters['integration-test-2']]

        # start clusters
        print('start clusters')
        start_cluster(ctx, 'integration-test-1')
        start_cluster(ctx, 'integration-test-2')
        print()

        # merge clusters
        print('merge clusters')
        resp = requests.post('http://{}:{}/merge'.format(lookup_ip(ctx, 1), ctx.config['httpPort']),
                             json=merge_json(ctx, cluster2_nodes))
        assert_that(resp.status_code == 200)
        mid = resp.json()['id']
        while resp.text.lower() != 'completed':
            time.sleep(1)
            resp = requests.get('http://{}:{}/merge/{}'.format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], mid))
        print('merge completed')
        print()

        # put more data
        print('prepare data')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(0, 100)})
        print('check prepared data on all nodes')
        for node in cluster1_nodes + cluster2_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(0, 100):
                got = get(ctx, node['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))
        print()

        # restart nodes
        print('restart nodes under merged cluster configuration')
        stop_cluster(ctx, 'integration-test-1')
        stop_cluster(ctx, 'integration-test-2')
        cluster_param_list = []
        for node in cluster1_nodes + cluster2_nodes:
            cluster_param_list.append('{}=http://{}:{}'.format(node['id'], node['ip'], ctx.config['raftPort']))
        cluster_param = ','.join(cluster_param_list)
        for node in cluster1_nodes + cluster2_nodes:
            start_cmd(ctx, node['id'], node['ip'], cluster_param)
        print()

        # put more data
        print('prepare more data')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(100, 200)})
        print('check all prepared data on all node')
        for node in cluster1_nodes + cluster2_nodes:
            print('checking node {}'.format(node['id']))
            for x in range(100, 200):
                got = get(ctx, node['id'], x).content.decode()
                assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))

    return test_suite(ctx, ['integration-test-1', 'integration-test-2'], __test__)


@task
def test_merge_single_coordinator_failure(ctx):
    """
    Test merge clusters under a single coordinator failure without harming the availability.
    """

    def __test__(ctx):
        clusters = extract_clusters(ctx)
        cluster1_nodes = [node for node in clusters['integration-test-1']]
        cluster2_nodes = [node for node in clusters['integration-test-2']]

        # start cluster 1 and prepare some data
        start_cluster(ctx, 'integration-test-1')
        print()
        print('inject failure to node {}'.format(cluster1_nodes[2]['id']))
        stop_cmd(ctx, cluster1_nodes[2]['ip'])
        print()
        print('prepare some data on cluster integration-test-1')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(0, 100)})
        print()

        # start cluster 2 but stop one node, and prepare some data
        start_cluster(ctx, 'integration-test-2')
        print()
        print('prepare some data on cluster integration-test-2')
        put(ctx, cluster2_nodes[0]['id'], {x: str(x) for x in range(100, 200)})
        print()

        # merge clusters
        print('merge clusters')
        resp = requests.post('http://{}:{}/merge'.format(lookup_ip(ctx, 1), ctx.config['httpPort']),
                             json=merge_json(ctx, cluster2_nodes))
        assert_that(resp.status_code == 200)
        merge_id = resp.json()['id']
        resp = requests.get('http://{}:{}/merge/{}'.format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], merge_id))
        assert_that(resp.text.lower() != 'completed', 'expect not completed')
        print()

        # put data
        print('prepare more data while merge is in progress')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(200, 300)})
        print()

        print('wait on merge completion')
        while resp.text.lower() != 'completed':
            time.sleep(1)
            resp = requests.get('http://{}:{}/merge/{}'.
                                format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], merge_id))
        print('merge is completed')
        print()

        # recover node
        print('recover failed node {}'.format(cluster1_nodes[2]['id']))
        start_node(ctx, cluster1_nodes[2]['id'])
        print()

        # wait for the failed node to catch up
        print('wait 3 seconds for failed node to catchup')
        time.sleep(3)

        # check prepared data on failed nodes
        print('check all prepared data on failed node')
        for x in range(0, 300):
            got = get(ctx, cluster1_nodes[2]['id'], x).content.decode()
            assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))

    return test_suite(ctx, ['integration-test-1', 'integration-test-2'], __test__)


@task
def test_merge_single_candidate_failure(ctx):
    """
    Test merge clusters under a single candidate failure without harming the availability.
    """

    def __test__(ctx):
        clusters = extract_clusters(ctx)
        cluster1_nodes = [node for node in clusters['integration-test-1']]
        cluster2_nodes = [node for node in clusters['integration-test-2']]

        # start cluster 1 and prepare some data
        start_cluster(ctx, 'integration-test-1')
        print('prepare some data on cluster integration-test-1')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(0, 100)})
        print()

        # start cluster 2 but stop one node, and prepare some data
        start_cluster(ctx, 'integration-test-2')
        print()
        print('inject failure to node {}'.format(cluster2_nodes[2]['id']))
        stop_cmd(ctx, cluster2_nodes[2]['ip'])
        print()
        print('prepare some data on cluster integration-test-2')
        put(ctx, cluster2_nodes[0]['id'], {x: str(x) for x in range(100, 200)})
        print()

        # merge clusters
        print('merge clusters')
        resp = requests.post('http://{}:{}/merge'.format(lookup_ip(ctx, 1), ctx.config['httpPort']),
                             json=merge_json(ctx, cluster2_nodes))
        assert_that(resp.status_code == 200)
        merge_id = resp.json()['id']
        resp = requests.get('http://{}:{}/merge/{}'.format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], merge_id))
        assert_that(resp.text.lower() != 'completed', 'expect not completed')
        print()

        # prepare mode data
        print('prepare more data while merge is in progress')
        put(ctx, cluster1_nodes[0]['id'], {x: str(x) for x in range(200, 300)})
        print()

        # wait on refresh phase
        print('wait on merge refreshing phase')
        while resp.text.lower() != 'refreshing':
            time.sleep(1)
            resp = requests.get('http://{}:{}/merge/{}'.
                                format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], merge_id))
        assert_that(resp.text.lower() == 'refreshing', 'expect: {}, got: {}'.format('refreshing', resp.text.lower()))
        print('enter refreshing phase')
        print()

        # recover node
        print('recover failed node {}'.format(cluster2_nodes[2]['id']))
        start_node(ctx, cluster2_nodes[2]['id'])
        print()

        # wait on completed
        print('wait on merge completion')
        while resp.text.lower() != 'completed':
            time.sleep(1)
            resp = requests.get('http://{}:{}/merge/{}'.
                                format(cluster1_nodes[0]['ip'], ctx.config['httpPort'], merge_id))
        assert_that(resp.text.lower() == 'completed', 'expect: {}, got: {}'.format('completed', resp.text.lower()))
        print('merge completed')
        print()

        # wait on failed node to catch up
        print('wait 3 seconds for the failed node to catch up')
        time.sleep(3)

        # check prepared data on failed nodes
        print('check all prepared data on the failed node')
        for x in range(0, 300):
            got = get(ctx, cluster2_nodes[2]['id'], x).content.decode()
            assert_that(got == str(x), 'expect: {}, got: {}'.format(x, got))

    return test_suite(ctx, ['integration-test-1', 'integration-test-2'], __test__)


@task
def test_merge_concurrent(ctx):
    """
    Test concurrent merge.
    """

    def __test__(ctx):
        clusters = extract_clusters(ctx)
        cluster1 = clusters['integration-test-1']
        cluster2 = clusters['integration-test-2']
        cluster3 = clusters['integration-test-3']

        start_cluster(ctx, 'integration-test-1')
        start_cluster(ctx, 'integration-test-2')
        start_cluster(ctx, 'integration-test-3')

        print()
        time.sleep(3)

        # let cluster 1 and cluster 3 to concurrently merge cluster 2
        print('try to use two cluster to merge another one concurrently')

        def merge(coord_clr):
            return requests.post('http://{}:{}/merge'.
                                 format(lookup_ip(ctx, coord_clr[0]['id']), ctx.config['httpPort']),
                                 json=merge_json(ctx, cluster2))

        success_clr = None
        succeed, failed = None, None
        for idx, resp in enumerate(concurrent.futures.ThreadPoolExecutor(2) \
                                           .map(merge, [cluster1, cluster3])):
            print(resp.content.decode())
            if resp.status_code == 200:
                succeed = resp
                success_clr = cluster1 if idx == 0 else cluster3
            else:
                failed = resp
        assert_that(succeed is not None and failed is not None, 'both failed' if succeed is None else 'both succeed')
        print()

        # put more data to check the merged cluster work as expected
        print('put data on successful coordinator cluster')
        put(ctx, success_clr[0]['id'], {x: str(x) for x in range(0, 100)})
        print('check data on original candidate cluster')
        for node in cluster2:
            print('check node' + str(node['id']))
            for x in range(0, 100):
                assert_that(get(ctx, node['id'], x).text == str(x), 'data incorrect on candidate cluster')
        print()

        print('put data on original candidate cluster')
        put(ctx, cluster2[0]['id'], {x: str(x) for x in range(100, 200)})
        print('check data on coordinator cluster')
        for node in success_clr:
            print('check node' + str(node['id']))
            for x in range(100, 200):
                assert_that(get(ctx, node['id'], x).text == str(x), 'data incorrect on coordinator cluster')

    return test_suite(ctx, ['integration-test-1', 'integration-test-2', 'integration-test-3'], __test__)
# ------------------INTEGRATION TESTS-------------------------


# ------------------EVALUATION-------------------------
@task
def eval_merge_log_size(ctx):
    """
    Evaluate merge efficiency by varying existing log amount.
    """

    def __eval__(ctx):
        coord_cnt = int(ctx.config['coord_cnt'])
        cand_cnt = int(ctx.config['cand_cnt'])

        start_cluster(ctx, 'integration-test-1')
        put(ctx, 1, {'coord-' + str(x): 'coord-' + str(x) for x in range(coord_cnt)})

        start_cluster(ctx, 'integration-test-2')
        put(ctx, 4, {'cand-' + str(x): 'cand-' + str(x) for x in range(cand_cnt)})
        print()

        # merge clusters
        print('merge clusters')
        cluster2_nodes = [node for node in extract_clusters(ctx)['integration-test-2']]
        resp = requests.post('http://{}:{}/merge'.format(lookup_ip(ctx, 1), ctx.config['httpPort']),
                             json=merge_json(ctx, cluster2_nodes))
        assert_that(resp.status_code == 200)
        merge_id = resp.json()['id']

        print('collect data')
        resp = get(ctx, 1, 'merge/stats/{}/start'.format(merge_id))
        assert_that(resp.status_code == 200, 'expect merge start time but failed querying')
        start_time = int(resp.content.decode())

        end_time = 0
        while end_time == 0:
            resp = get(ctx, 1, 'merge/stats/{}/finish'.format(merge_id))
            if resp.status_code == 200:
                end_time = int(resp.content.decode())
        ctx.config['merge_latency'] = end_time - start_time
        print()

    repeat = 3
    coordinator_log_cnt = [100]
    candidate_log_cnt = [x for x in range(500, 5500, 500)]

    merge_latency_result = {}
    for coord_cnt in coordinator_log_cnt:
        for cand_cnt in candidate_log_cnt:
            merge_latency_sum = .0
            for _ in range(repeat):
                ctx.config['coord_cnt'] = coord_cnt
                ctx.config['cand_cnt'] = cand_cnt
                test_suite(ctx, ['integration-test-1', 'integration-test-2'], __eval__, False)

                print()
                print('result for {} and {}:'.format(coord_cnt, cand_cnt))

                print('merge latency in ms: {}'.format(ctx.config['merge_latency']))
                merge_latency_sum += ctx.config['merge_latency']

            print()
            print('average result for {} and {}:'.format(coord_cnt, cand_cnt))

            avg_merge_latency = merge_latency_sum / repeat
            merge_latency_result[cand_cnt] = avg_merge_latency
            print('average merge latency in ms: {}'.format(avg_merge_latency))
            print()

    print()
    print('----------AFTER TEST: CLEAN ENVIRONMENT----------')
    for clr in ['integration-test-1', 'integration-test-2']:
        stop_cluster(ctx, clr)
    print('clean running files')
    clean(ctx)
    print('--------------------------------------------------')
    print()

    print('merge latency in ms: ' + str(merge_latency_result))


@task
def eval_merge_batch_size(ctx):
    """
    Evaluate merge efficiency by varying log pull size.
    """

    def __eval__(ctx):
        start_cluster(ctx, 'integration-test-1')
        start_cluster(ctx, 'integration-test-2')

        print()
        print("prepare data")
        put(ctx, 4, {x: str(x) for x in range(5000)})
        print()

        # merge clusters
        print('merge clusters')
        cluster2_nodes = [node for node in extract_clusters(ctx)['integration-test-2']]
        resp = requests.post('http://{}:{}/merge'.format(lookup_ip(ctx, 1), ctx.config['httpPort']),
                             json=merge_json(ctx, cluster2_nodes))
        assert_that(resp.status_code == 200)
        merge_id = resp.json()['id']

        print('collect data')
        resp = get(ctx, 1, 'merge/stats/{}/start'.format(merge_id))
        assert_that(resp.status_code == 200, 'expect merge start time but failed querying')
        start_time = int(resp.content.decode())

        end_time = 0
        while end_time == 0:
            resp = get(ctx, 1, 'merge/stats/{}/finish'.format(merge_id))
            if resp.status_code == 200:
                end_time = int(resp.content.decode())
        ctx.config['merge_latency'] = end_time - start_time
        print()

    repeat = 3
    batch_size = [x for x in range(100, 1001, 100)]

    merge_latency_result = {}
    for sz in batch_size:
        merge_latency_sum = .0
        for _ in range(repeat):
            ctx.config['pullBatchSize'] = sz
            test_suite(ctx, ['integration-test-1', 'integration-test-2'], __eval__, False)

            print()
            print('result for pull batch size {}:'.format(sz))

            print('merge latency in ms: {}'.format(ctx.config['merge_latency']))
            merge_latency_sum += ctx.config['merge_latency']

        print()
        print('average result for {}:'.format(sz))

        avg_merge_latency = merge_latency_sum / repeat
        merge_latency_result[sz] = avg_merge_latency
        print('average merge latency in ms: {}'.format(avg_merge_latency))
        print()

    print()
    print('----------AFTER TEST: CLEAN ENVIRONMENT----------')
    for clr in ['integration-test-1', 'integration-test-2']:
        stop_cluster(ctx, clr)
    print('clean running files')
    clean(ctx)
    print('--------------------------------------------------')
    print()

    print('merge latency in ms: ' + str(merge_latency_result))
# ------------------EVALUATION-------------------------
