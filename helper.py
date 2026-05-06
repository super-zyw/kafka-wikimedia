import json
import sys
from pprint import pformat

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

def stats_cb(stats_json_str):
    stats_json = json.loads(stats_json_str)
    print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


def print_usage_and_exit(program_name):
    sys.stderr.write('Usage: %s [options..] <bootstrap-brokers> <group> <topic1> <topic2> ..\n' % program_name)
    options = '''
     Options:
      -T <intvl>   Enable client statistics at specified interval (ms)
    '''
    sys.stderr.write(options)
    sys.exit(1)


