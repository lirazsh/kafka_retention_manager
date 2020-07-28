#
#   Topic Async Limit Coordinator
#   Dynamically adjusts topic retention time to maintain acceptable disk usage values.
#
#   by Liraz S
#


import json
import os
import subprocess
import threading
#import pdb

from time import sleep
from datetime import datetime
from kazoo.client import KazooClient
#from kazoo.recipe.watchers import ChildrenWatch
from kazoo.exceptions import NoNodeError, NodeExistsError

config = {'config_path': '/etc/talc/talc.json'}


def load_config():
    global config
    try:
        with open(config['config_path']) as f:
            config.update(json.load(f))
        log_msg("load_config: read the following config from file ({0}):\n{1}".format(config['config_path'], json.dumps(config, indent=3)))
    except Exception as e:
        log_msg("load_config: unable to read config file: {0} ".format(str(e)))
        raise e


def log_msg(msg, log_lock=None):
    time_str = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    date_str = datetime.now().strftime('%Y-%m-%d')
    if log_lock is not None:
        log_lock.acquire()
    try:
        if not os.path.isdir(os.path.dirname(config['log_path'])):
            os.mkdir(os.path.dirname(config['log_path']))
        with open('{0}_{1}.log'.format(config['log_path'], date_str), 'a+') as f:
            f.write('{0} - {1}\n'.format(time_str, msg))
    except Exception as e:
        print("{0} - Unable to write to log file: {1}".format(time_str, str(e)))
        raise e
    finally:
        if log_lock is not None:
            log_lock.release()


def get_topics():
    try:
        topics = (subprocess.check_output(['{0}/bin/kafka-topics.sh'.format(config['kafka_home']), '--list', '--zookeeper', config['zookeeper']])).split('\n')
        if topics is None:
            log_msg("get_topics: no topics found!")
            exit(1)
        config['excluded_topics'].append('')
        topics = filter(lambda x: x not in config['excluded_topics'], topics)
        log_msg("get_topics: discovered the following topics:\n{0}".format(topics))
        log_msg("get_topics: excluded the following topics:\n{0}".format(json.dumps(config['excluded_topics'])))
        return topics
    except Exception as e:
        log_msg('get_topics: unable to get topic list: \"{0}/bin/kafka-topics.sh --list --zookeeper {1} zookeeper\": '.format(config['kafka_home'], config['zookeeper']) + str(e))
        raise e


def get_consumption(my_dir):
    try:
        fs = os.statvfs(my_dir)
    except Exception as e:
        log_msg("get_consumption: cannot get disk size: " + str(e))
        raise e
    return int(round(100 - (100 * (float(fs.f_bavail) / float(fs.f_blocks)))))


def change_retention(topics, new_retention):
    bin_file = os.path.join(config['kafka_home'], 'bin/kafka-configs.sh')

    # Do not breach maximum
    if new_retention > config['max_retention_ms']:
        new_retention = config['max_retention_ms']

    # Do not breach minimum
    if new_retention < config['min_retention_ms']:
        new_retention = config['min_retention_ms']

    # Apply the change to topics
    changed_topics = []
    failed_topics = []
    threads = []
    lock = threading.RLock()
    log_lock = threading.RLock()

    for i in range(0, config['num_threads']):
        worker_topics = list(filter(lambda x: topics.index(x) % config['num_threads'] == i, topics))
        t = threading.Thread(name=str(i), target=worker, args=(worker_topics, changed_topics, failed_topics,
                                                               "{0} --entity-type topics --zookeeper {1} --alter --add-config retention.ms={2}".format(bin_file, config['zookeeper'], new_retention), lock, log_lock))
        threads.append(t)
        t.start()

    for t in threading.enumerate():
        if t not in threads:
            continue
        # Wait for thread to join. allocated 5s + 10s per topic.
        t.join(5 + 10*(float(len(topics))/config['num_threads']))
        if t.is_alive():
            log_msg("Thread {} timed out.".format(t))

    return changed_topics, failed_topics


def worker(topics, changed_topics, failed_topics, cmd, lock, log_lock):
    while len(topics) > 0:
        #log_msg("worker {}: {} topics left in my list: {}".format(threading.currentThread().getName(), len(topics), topics), log_lock)
        topic = topics.pop()
        exe = cmd + " --entity-name {0}".format(topic)
        if config['debug']:
            print(exe)
        try:
            # with lock:
            subprocess.check_output(exe, shell=True)
            changed_topics.append(topic)
            with log_lock:
                log_msg("worker {0}: successfully altered topic \'{1}\'".format(threading.currentThread().getName(), topic), log_lock)
        except Exception as e:
            with log_lock:
                log_msg("worker {0}: unable to alter topic \'{1}\': {2}".format(threading.currentThread().getName(), topic, str(e)), log_lock)
            failed_topics.append(topic)
    log_msg("worker {}: FINISHED".format(threading.currentThread().getName()), log_lock)


# current: % disk usage
# target: desired % disk usage
# retention: current retention time
def calc_retention(current, target, retention):
    if current <= 0:
        current = 1
    if target > 100:
        target = 100

    spread = target - current
    change = float(spread) / float(current)
    new_retention = int(round((retention * change))) + retention
    if new_retention > config['max_retention_ms']:
        new_retention = config['max_retention_ms']
    if new_retention < config['min_retention_ms']:
        new_retention = config['min_retention_ms']
    return new_retention


def get_retention_zk(zk):
    try:
        ret = int(zk.get("/talc/retention")[0])
        zk_topics = filter(lambda x: x not in config['excluded_topics'], zk.get_children('/config/topics'))
        for topic in zk_topics:
            topic_ret = int(eval(zk.get("/config/topics/{}".format(topic))[0])['config']['retention.ms'])
            if topic_ret != ret:
                log_msg("get_retention_zk: found mismatch between topic {} ({}) and last known state ({}). Resetting"
                        " retention to default value...".format(topic, str(topic_ret), str(ret)))
                return -1
        log_msg("get_retention_zk: retention in zookeeper: {}h".format(str(ret/3600000)))
        return ret
    except NoNodeError:
        log_msg("get_retention_zk: could not find node in zookeeper.")
        return -1


def set_retention_zk(zk, new_retention):
    try:
        zk.set("/talc/retention", str(new_retention))
        log_msg("set_retention_zk: retention set to {}".format(str(new_retention)))
    except NoNodeError:
        log_msg("set_retention_zk: /talc/retention does not exist. Creating and setting...")
        zk.create("/talc/retention", str(new_retention), makepath=True)


def set_state_file(retention_file, new_retention, leader):
    try:
        with open(retention_file, 'w+') as f:
            new_retention_hours = (new_retention / (1000 * 60 * 60)) % 24
            new_retention_days = (new_retention / (1000 * 60 * 60 * 24))
            my_config = {'topics_retention_ms': new_retention, 'leader_id': leader, 'topics_retention_human': '{0}d{1}h'.format(new_retention_days, new_retention_hours), 'topics_retention': '{0}'.format(new_retention / 3600000)}
            json.dump(my_config, f)
        log_msg("set_retention_file: updated retention file with new value: {0}ms".format(new_retention))
    except Exception as e:
        log_msg("set_retention_file: unable to update retention file with new value: {0}".format(str(e)))
        raise e


def get_topics_config():
    try:
        bin_file = os.path.join(config['kafka_home'], 'bin/kafka-configs.sh')
        log_msg(subprocess.check_output("{0} --entity-type topics --zookeeper {1} --describe".format(bin_file, config['zookeeper']), shell=True))
    except Exception as e:
        log_msg("get_topics_config: unable to load topic configs: {0}".format(str(e)))
        raise e


def is_leader(zk):
    # Get current leader
    try:
        leader = eval(zk.get("/controller")[0])['brokerid']
    except Exception as e:
        log_msg("get_leader_zk: unable to get leader: {0}".format(str(e)))
        raise e

    # If I'm not kafka leader, exit.
    if leader != int(config["broker_id"]):
        log_msg("I am not leader. leader is broker#{0}. Going to sleep...".format(leader))
        try:
            os.remove(config['state_file'])
        except OSError:
            pass
        except Exception as e:
            log_msg("unable to remove state file: {0}".format(str(e)))
            raise e
        return False
    return True


def start_zk_connection():
    try:
        zk = KazooClient(hosts=config['zookeeper'])
        zk.start()
    except Exception as e:
        log_msg("Unable to connect to zookeeper: {0}".format(str(e)))
        raise e
    return zk


def eval_retention(curr_retention, curr_consumption):
    if curr_retention > 0:
        target_watermark = (config['high_watermark'] + config['low_watermark'])/2
        new_retention = calc_retention(curr_consumption, target_watermark, curr_retention)

        if new_retention >= config['max_retention_ms'] and curr_retention == config['max_retention_ms']:
            log_msg("eval_retention: Unable to increase retention, already at maximum: {0}h".format(config['max_retention_ms']/3600000))
            return curr_retention

        if new_retention <= config['min_retention_ms'] and curr_retention == config['min_retention_ms']:
            log_msg("eval_retention: Unable to decrease retention, already at minimum: {0}h".format(config['min_retention_ms']/3600000))
            return curr_retention
    else:
        new_retention = config['default_initial_retention']
        log_msg("Retention not set. Setting retention to default value: {0}ms".format(config['default_initial_retention']))
    return new_retention


def update_retention(zk, new_retention):
    try:
        set_state_file(config['state_file'], new_retention, config['broker_id'])
        set_retention_zk(zk, new_retention)
    except Exception as e:
        log_msg("update_retention: unable to update new retention")
        raise e


def set_topic_retention(new_retention):
    topics = get_topics()
    changed_topics = []
    failed_topics = []
    try:
        changed_topics, failed_topics = change_retention(topics, new_retention)
        log_msg("Finished changing retention successfully for {0} topics.".format(len(changed_topics)))
        log_msg("Failed changing retention for {0} topics.".format(len(failed_topics)))
    except Exception as e:
        log_msg("Failure changing topic retention: {0}".format(str(e)))
    finally:
        if changed_topics is not None and len(changed_topics) > 0:
            log_msg("Successfully adjusted the following topics: {0}".format(str(changed_topics)))
        if failed_topics is not None and len(failed_topics) > 0:
            log_msg("Failed adjusting the following topics: {0}".format(str(failed_topics)))
        get_topics_config()


def zk_update_node_consumption(zk, consumed):
    try:
        zk.create("/talc/nodes/{}".format(config["broker_id"]), str(consumed), makepath=True)
        log_msg("zk_update_node_consumption: znode \"/talc/nodes/{}\" "
                "set to current consumption: {}%".format((config["broker_id"]), str(consumed)))
    except NodeExistsError:
        zk.set("/talc/nodes/{}".format(config["broker_id"]), str(consumed))
        log_msg("zk_update_node_consumption: znode exists. This might indicate an issue with the master."
                " updated zk with current consumption: {}%".format(str(consumed)))
    log_msg("zk_update_node_consumption: updated consumption in ZK successfully.")


def zk_get_nodes_consumption(zk):
    report = {}
    path = '/talc/nodes'

    try:
        lock = zk.Lock(path, config["broker_id"])
        with lock:
            zk_slave_nodes = zk.get_children(path, include_data=False)
            for slave in zk_slave_nodes:
                if "lock" in slave:
                    continue
                report[slave] = int(zk.get('{}/{}'.format(path, slave))[0])
                zk.delete('{}/{}'.format(path, slave))
    except Exception as e:
        log_msg("zk_get_nodes_consumption: failed getting node reports from zk")
        raise e

    log_msg("zk_get_nodes_consumption: report: {}".format(json.dumps(report, indent=4)))
    return report


def compute_new_retention(zk):
    dec_required = False
    inc_required = False
    curr_retention = get_retention_zk(zk)
    new_min_retention = curr_retention
    new_max_retention = curr_retention

    for node, consumption in zk_get_nodes_consumption(zk).iteritems():
        if config['high_watermark'] >= consumption >= config['low_watermark']:
            log_msg("compute_new_retention: broker {0} usage between {1}-{2}%: {3}%. no change in retention required.".format(node, config['low_watermark'], config['high_watermark'], str(consumption)))
            continue
        log_msg("compute_new_retention: broker {0} usage breached boundaries of {1}-{2}%: {3}%. Considering retention adjustment...".format(node, config['low_watermark'], config['high_watermark'], str(consumption)))
        evaluated_ret = eval_retention(curr_retention, consumption)
        log_msg("compute_new_retention: Calculated target retention {}h for broker: \"{}\"".format(evaluated_ret/3600000, node))
        if evaluated_ret < new_min_retention:
            new_min_retention = evaluated_ret
            dec_required = True
        elif evaluated_ret > new_max_retention:
            new_max_retention = evaluated_ret
            inc_required = True
    if dec_required:
        return new_min_retention
    if inc_required:
        return new_max_retention
    return -1


def all_nodes_reported(zk):
    num_brokers = len(zk.get_children('/brokers/ids'))
    num_reports = len(zk.get_children('/talc/nodes'))

    iter = 0
    while num_brokers != num_reports:
        if iter >= 3:
            log_msg("all_nodes_reported: failed to get all reports in a timely manner...")
            return False
        log_msg("all_nodes_reported: received {}/{} reports. Trying again in 20 seconds...".format(num_reports,
                                                                                                   num_brokers))
        iter += 1
        sleep(20)
        num_reports = len(zk.get_children('/talc/nodes'))

    log_msg("all_nodes_reported: received {}/{} reports.".format(num_reports, num_brokers))
    return True


def main():
    load_config()
    curr_consumption = get_consumption(config['kafka_data_dir'])

    zk = start_zk_connection()
    try:
        zk_update_node_consumption(zk, curr_consumption)
        if is_leader(zk):
            if config['all_nodes_only'] and not all_nodes_reported(zk):
                msg = "main: master - all_nodes_only is enabled: not all nodes reported. Going to sleep..."
                log_msg(msg)
                raise Exception(msg)

            new_retention = compute_new_retention(zk)
            if new_retention != -1:
                update_retention(zk, new_retention)
                set_topic_retention(new_retention)
            else:
                log_msg("main: master - no change in retention required on any nodes. Going to sleep...")
    finally:
        zk.stop()
        zk.close()
    log_msg("main: Done!")


if __name__ == "__main__":
    main()
