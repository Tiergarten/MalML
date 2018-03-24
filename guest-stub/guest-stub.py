import os
import urllib
from subprocess import Popen, PIPE
import time
import requests

# TODO read this from env
DETONATOR_HTTPD = 'http://192.168.1.130:5000'
GET_AGENT_URI = '{}/agent-stub/get-agent'.format(DETONATOR_HTTPD)
ERR_URI = '{}/agent-stub/error'.format(DETONATOR_HTTPD)

if os.name == 'nt':
    PYTHON_BIN = "C:\\Python27\\python.exe"
else:
    PYTHON_BIN = "/usr/local/bin/python"


def get_agent(agent_uri):
    local_filename = "_agent.py"
    urllib.urlretrieve(GET_AGENT_URI, local_filename)
    return local_filename


def exec_agent(agent_path):
    cmd = [PYTHON_BIN, agent_path]

    proc = Popen(cmd, stdout=PIPE)
    out, err = proc.communicate()

    post_data = { 'exit_code': proc.returncode, 'output': out }

    if out.returncode != 0:
        r = requests.post(ERR_URI, data=post_data)

if __name__ == '__main__':
    time.sleep(10)
    agent = get_agent(GET_AGENT_URI)
    exec_agent(agent)

