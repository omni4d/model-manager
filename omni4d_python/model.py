import yaml
from urllib.request import urlopen
from pathlib import Path


def model_from_url(url):
    file = urlopen(url)
    return yaml.load(file)


def model_from_file(file_path):
    file = Path(file_path).open('r')
    return yaml.load(file)
