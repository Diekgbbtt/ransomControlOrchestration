# /bin/env python3.12

from json import load as json_load
from orchestration import controlClass
from progress.bar import IncrementalBar
import time

def load_config():
    try:
        with open('config.json', 'r') as cfg:
                cfg_dict = json_load(cfg)
        return cfg_dict

    except Exception as e:
            print(f"Error opening config: {str(e)}")


def controlDatabase(check: controlClass):

    check.start()
    while( not check.finish()):
        print("controllo in corso")


# display a bar that shows the progress of the process
def print_process_status():
        bar = IncrementalBar(suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100)
        for i in bar.iter(range(200)):
            time.sleep(0.01)