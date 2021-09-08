import requests
import json
import sys
import logging
import argparse

import yaml


def parse_args(args):
    """
    Parses command line arguments based on a list of dictionaries

    :param args: The list of dictionaries defining command line args. Format:
    [
        {
            # 'definition' can be a:
            #   list containing the abbreviated and full names of an optional arg; ['-abrev_name', '--full_name']
            #   string name of an optional arg; '-abrev_name' OR '--full_name'
            #   string name of a required positional arg; 'full_name' ()
            'definition': ['-abrev_name', '--full_name'],
            # 'params' can be any parameters of argparse.ArgumentParser.add_argument().
            #   https://docs.python.org/3/library/argparse.html#the-add-argument-method
            'params': {
                'default': None, # default value
                'help': '<Your explanation of what this arg means and any other helpful documentation>'
                # ... more params
            }
        },
        {
            # ...another arg here
        }
    ]
    :return: dictionary of command line arg values
    """
    ap = argparse.ArgumentParser()
    for item in args:
        if isinstance(item["definition"], list):
            ap.add_argument(*item["definition"], **item["params"])
        else:
            ap.add_argument(item["definition"], **item["params"])
    return vars(ap.parse_args())


def parse_yaml(file_path):
    """
    Parses a yaml file

    :param file_path: Path of the yaml file to parse
    :return: The parsed yaml file
    """
    with open(file_path, "r") as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
            return parsed_yaml
        except yaml.YAMLError as exc:
            sys.exit(exc)


def get_logger(
    name,
    stream=sys.stdout,
    style="{",
    frmt="{name} [{levelname}]:: {message}",
    level="INFO",
):
    """
    Create a logger. If the logger already exists, clear the handlers
        and re-add them according to the parameters provided.

    :param name: The name of the logger
    :param stream: The stream to log to
    :param style: Any style from Python's standard logging module
                  (see https://docs.python.org/3/library/logging.html#formatter-objects for details)
    :param frmt: The log format string (https://docs.python.org/3/library/logging.html#logging.Formatter.format)
    :param level: A standard Python logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :return: A logging.Logger object
    """
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    handler = logging.StreamHandler(stream=stream)
    formatter = logging.Formatter(frmt, style=style)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    return logger


def api_get_request(url):
    """
    Make a GET request

    :param url: The URL to request
    :return: The text attribute of the response
    """
    response = requests.get(url)
    response.raise_for_status()
    response_text = json.loads(response.text)
    return response_text


def format_response(response, key_map):
    """
    Takes a nested dictionary (a record), or list of (potentially
        nested) dictionaries (records); and filters, flattens, and
        renames the key-value pairs of each record according to another
        dictionary (key map) provided by the user.

    :param response: An API response of a single record (a dictionary)
        or multiple records (a list of dictionaries)
    :param key_map: A (potentially nested) dictionary used to filter
        and flatten each record in the response.
    :return: Same type as response; with each record filtered,
        flattened, and renamed according to the key_map
    """
    if isinstance(response, dict):
        formatted_response = _format_record(response, key_map)
    else:
        formatted_response = list()
        for record in response:
            formatted_record = _format_record(record, key_map)
            formatted_response.append(formatted_record)
    return formatted_response


def _format_record(record, key_map):
    """
    Helper function to format a single record as described in
        format_response

    :param record: A (potentially nested) dictionary representing a
        single record from an API response
    :param key_map: A (potentially nested) dictionary used to filter
        and flatten the record
    :return: A dictionary; filtered, flattened, and renamed according
        to the key_map
    """
    formatted_record = dict()
    for k, v in key_map.items():
        if k in record:
            _format_key(record, k, v, formatted_record)
    return formatted_record


def _format_key(record, key, value, formatted_record):
    """
    A recursive helper function to handle nested dictionaries from
        _format_record

    :param record: The record received from _format_record
    :param key: A key in the key_map from _format_record
    :param value: The corresponding value in the key_map from
        _format_record
    :param formatted_record: A dictionary; filtered, flattened, and
        renamed according to the key_map from _format_record
    :return: None
    """
    if isinstance((sub_record := record.get(key)), dict):
        for sub_key, sub_value in value.items():
            _format_key(sub_record, sub_key, sub_value, formatted_record)
    else:
        formatted_record[value.get("col_name") or key] = record.get(key)


def get_data_types(key_map, data_types=None):
    if data_types is None:
        data_types = dict()
    for k in key_map:
        if isinstance(key_map[k], dict) and not key_map[k].get("data_type"):
            get_data_types(key_map[k], data_types=data_types)
        else:
            data_types[key_map[k].get("col_name", k)] = key_map[k][
                "data_type"
            ].upper()
    return data_types
