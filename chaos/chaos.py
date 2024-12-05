from datetime import datetime, timedelta
import threading
import time
import random
import docker
import yaml
import signal
import queue

controllers = {
    "map_filter": {
        "query_one_games": "MFGQ1",
        "query_two_games": "MFGQ2",
        "query_three_games": "MFGQ3",
        "query_four_games": "MFGQ4",
        "query_five_games": "MFGQ5",
        "query_three_reviews": "MFRQ3",
        "query_four_reviews": "MFRQ4",
        "query_five_reviews": "MFRQ5",
    },
    "query_one": {
        "stage_two": "Q1S2",
        "stage_three": "Q1S3"
    },
    "query_two": {
        "stage_two": "Q2S2",
        "stage_three": "Q2S3"
    },
    "query_three": {
        "stage_two": "Q3S2",
        "stage_three": "Q3S3"
    },
    "query_four": {
        "stage_two": "Q4S2",
        "stage_three": "Q4S3"
    },
    "query_five": {
        "stage_two": "Q5S2",
        "stage_three": "Q5S3"
    },
}

extras = [
    f"manager_{i}" for i in range(1,4)
]

class Node:
    UNKNOWN = "UNKNOWN"
    UP = "UP"
    DOWN = "DOWN"

    def __init__(self, name: str):
        self.name = name
        self.last_killed = datetime.min
        self.state = Node.UNKNOWN

    def __repr__(self) -> str:
        return f"Node;name={self.name}"
    
    def __str__(self) -> str:
        return f"Node;name={self.name}"
    

class Chaos:

    def __init__(self, nodes: list[Node]):
        self._client = docker.from_env()
        self._nodes = nodes
        self._stop = threading.Event()
        self._exp: threading.Thread | None = None
        self._tomonitor = queue.Queue()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.stop()
    
    def __del__(self):
        self._client.close()

    def _monitor(self):
        monitoring = []
        while not self._stop.is_set():
            try:
                node = self._tomonitor.get_nowait()
                monitoring.append(node)
            except Exception:
                pass 

            for node in monitoring:
                try:
                    container = self._client.containers.get(node.name)
                    status = container.status
                    if status.lower() == 'running':
                        print(f"REVIVED: {node.name}")
                        monitoring.remove(node)
                    else:
                        print(f"NODE: {node.name} still dead, status: {status}")
                except Exception as e:
                    print(f"Can't get status of {node.name}. Reason: {e}")
                    monitoring.remove(node)
            time.sleep(3)

    def monitor(self): 
        threading.Thread(target=self._monitor).start()

    def kill_node(self, node: Node) -> None:
        try:
            c = self._client.containers.get(node.name)
            c.kill()
            node.last_killed = datetime.now()
            node.state = Node.DOWN
            self._tomonitor.put(node)
            print(f"KILLED: {node.name}")
        except Exception as e:
            print(f"Can't kill {node.name}. Reason: {e}")

    def _in_thread(self, target, args) -> None:
        if self._exp and self._exp.is_alive():
            raise Exception("The experiment is already running")
        
        self.reset()
        self._exp = threading.Thread(target=target, args=args)
        self._exp.start()

    def _start_random(self, timeout: int, cooldown: int, seed:int = 0)->None:
        r = random.Random(seed)
        cooldown_delta = timedelta(seconds=cooldown)
        while not self._stop.is_set():
            eligible_nodes = [
                    node for node in self._nodes
                    if datetime.now() - node.last_killed >= cooldown_delta
                ]
            if not eligible_nodes:
                time.sleep(timeout)
                continue
            node = r.choice(eligible_nodes)
            self.kill_node(node)

            time.sleep(timeout)

    def start_random(self, timeout: int, cooldown: int, seed:int = 0) -> None:
        self.monitor()
        self._in_thread(self._start_random, (timeout, cooldown, seed))

    def _start_stepped(self, step: int) -> None:
         while not self._stop.is_set():
            for node in self._nodes:
                if self._stop.is_set():
                    return
                self.kill_node(node)
                time.sleep(step)

    def start_stepped(self, step: int) -> None:
       self.monitor()
       self._in_thread(self._start_stepped, (step, ))

    def stop(self):
        self._stop.set()

    def reset(self):
        self._stop.clear()

def traverse(controllers: dict, architecture: dict):
    r = []
    if not isinstance(controllers, dict):
        for i in range(1, architecture['partition_amount'] + 1):
            r.append(Node(f"node_{controllers}_{i}".lower()))
        return r
    for key, value in controllers.items():
        v = traverse(value, architecture[key])
        for x in v:
            r.append(x)
    return r

def build_nodes() -> list[Node]:
    with open("./architecture.yaml", "r") as f:
        architecture = yaml.safe_load(f)
    return traverse(controllers, architecture)

w = Chaos(build_nodes())
m = Chaos([Node(name) for name in extras])

w.start_random(5, 60, 42)
m.start_stepped(60)
# while True:
#     time.sleep(500)