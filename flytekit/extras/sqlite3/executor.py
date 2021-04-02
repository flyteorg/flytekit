#!/usr/bin/env python

import argparse







if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('integers', metavar='N', type=int, nargs='+',
                        help='an integer for the accumulator')
    parser.add_argument('--inputs', dest='accumulate', action='store_const',
                        const=sum, default=max,
                        help='sum the integers (default: find the max)')

    parser.add_argument('--outputs', dest='accumulate', action='store_const',
                        const=sum, default=max,
                        help='sum the integers (default: find the max)')

    inputs_path, output_prefix, data_prefix, task_template_path = "", "", "", ""

    # call load entrypoint, call dispatch_execute