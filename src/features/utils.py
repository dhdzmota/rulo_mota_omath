#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yaml
import os
from src import config


config_path = os.path.join(
        config.PRJ_DIR, 'src/features/features.config')


def get_config():
    with open(config_path, "r") as f:
        config_features = yaml.load(f, Loader=yaml.FullLoader)
        return config_features