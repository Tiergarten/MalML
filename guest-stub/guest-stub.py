import os
import urllib

GET_AGENT_URI = "http://localhost:5000/agent-stub/get-agent"

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
    print cmd

    # TODO: Wait for finish, or interrupt after pre-defined time?
    os.execve(cmd[0], cmd, os.environ)
    os.exit(0)


if __name__ == '__main__':
    agent = get_agent(GET_AGENT_URI)
    exec_agent(agent)

