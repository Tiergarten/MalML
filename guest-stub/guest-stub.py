import urllib

AGENT_URI = "http://localhost:8080/static/agent.py"

if __name__ == '__main__':
	urllib.urlretrieve(AGENT_URI)
	from detonate_agent import *
	detonate_agent()