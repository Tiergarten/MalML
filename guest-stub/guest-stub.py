import os
import urllib
import subprocess

# TODO read this from env
GET_AGENT_URI = "http://192.168.1.130:5000/agent-stub/get-agent"

if os.name == 'nt':
    PYTHON_BIN = "C:\\Python27\\python.exe"
else:
    PYTHON_BIN = "/usr/local/bin/python"


def get_agent(agent_uri):
    local_filename = "_agent.py"
    # TODO: how do i check the return code?
    urllib.urlretrieve(GET_AGENT_URI, local_filename)
    return local_filename


def exec_agent(agent_path):
    cmd = [PYTHON_BIN, agent_path]

    # TODO: Wait for finish, or interrupt after pre-defined time?
    subprocess.Popen(cmd).wait()


if __name__ == '__main__':
    agent = get_agent(GET_AGENT_URI)
    exec_agent(agent)

