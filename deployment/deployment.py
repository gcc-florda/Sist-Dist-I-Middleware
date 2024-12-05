import yaml
import copy
from pathlib import Path
import shutil

CLIENTS_NR = 3
MANAGERS_NR = 4

def represent_none(self, _):
    return self.represent_scalar('tag:yaml.org,2002:null', '')

yaml.add_representer(type(None), represent_none)

with open("./deployment/rabbit_service.yaml", "r") as f:
    rabbit_compose = yaml.safe_load(f)

with open("./deployment/client_compose.yaml" ,"r") as f:
    client_compose = yaml.safe_load(f)

with open("./deployment/server_compose.yaml","r") as f:
    server_compose = yaml.safe_load(f)

with open("./deployment/worker_compose.yaml","r") as f:
    worker_compose = yaml.safe_load(f)

with open("./deployment/controller.yaml") as f:
    controller = yaml.safe_load(f)

with open("./architecture.yaml", "r") as f:
    architecture = yaml.safe_load(f)

with open("./deployment/infra_manager.yaml", "r") as f:
    manager_compose = yaml.safe_load(f)


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

def create_node_definition(node_name: str):
    cpy = copy.deepcopy(worker_compose)
    cpy['worker']['container_name'] = f"node_{node_name.lower()}"
    cpy['worker']['volumes'][-3] = f"./configs/controller_node_{node_name}.yaml:/app/controllers.yaml"
    cpy['worker']['volumes'][-2] = f"./worker_files/node_{node_name}/data:/app/data"
    cpy['worker']['volumes'][-1] = f"./worker_files/node_{node_name}/metadata:/app/metadata"
    Path(f"./worker_files/node_{node_name}").mkdir(exist_ok=True)
    Path(f"./worker_files/node_{node_name}/data").mkdir(exist_ok=True)
    Path(f"./worker_files/node_{node_name}/metadata").mkdir(exist_ok=True)
    return {
        node_name.lower(): cpy['worker']
    }

def save_config(controller_name:str, i:int):
    cpy = copy.deepcopy(controller)
    cpy["controllers"][0]["type"] = controller_name
    cpy["controllers"][0]["readFromPartition"] = i

    with open(f"./configs/controller_node_{controller_name}_{i}.yaml", "w+") as cfg:
        yaml.dump(cpy, cfg, default_flow_style=False)


def traverse(controllers: dict, architecture: dict):
    r = []
    if not isinstance(controllers, dict):
        for i in range(1, architecture['partition_amount'] + 1):
            r.append(create_node_definition(f"{controllers}_{i}"))
            save_config(controllers, i)
        return r
    for key, value in controllers.items():
        v = traverse(value, architecture[key])
        for x in v:
            r.append(x)
    return r

shutil.rmtree("./worker_files", ignore_errors=True)
Path("./worker_files").mkdir(exist_ok=True)

shutil.rmtree("./configs", ignore_errors=True)     
Path("./configs").mkdir(exist_ok=True)

shutil.rmtree("./results", ignore_errors=True)
Path("./results").mkdir(exist_ok=True)

compose = {
    "services": {
        **rabbit_compose,
        **server_compose,
    }, 
    "volumes": {
        "rabbitmq_data": None
    }
}


for i in range(1, CLIENTS_NR + 1):
    Path(f"./results/client_{i}").mkdir(exist_ok=True)
    cpy = copy.deepcopy(client_compose)
    cpy["client"]["container_name"] = f"client_{i}"
    cpy["client"]["volumes"][-1] = f"./results/client_{i}:/app/results"
    compose["services"][f"client_{i}"] = cpy["client"]

for i in range(1, MANAGERS_NR + 1):
    cpy = copy.deepcopy(manager_compose)
    cpy["manager"]["container_name"] = f"manager_{i}"
    cpy["manager"]["environment"][0] = f"MANAGER_ID={i}"
    compose["services"][f"manager_{i}"] = cpy["manager"]

for worker_def in traverse(controllers, architecture):
    compose["services"] = {
        **compose["services"],
        **worker_def
    }

with open("./out_compose.yaml", "w+") as out_file:
    yaml.dump(compose, out_file, default_flow_style=False)

