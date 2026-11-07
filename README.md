# Apache Airflow on a VM (without Docker)

This repo documents how to host **Apache Airflow 2.10.2** on an Ubuntu Linux VM without Docker, using a Python virtual environment. It also contains a sample DAG (`airflow/dag/sample.py`) for an extract → transform → load pipeline.

## Prerequisites

- An Ubuntu VM with SSH access (Airflow is memory-hungry — 4 GB RAM is a comfortable minimum)
- Git installed on the VM

---

## 1. Connect to the VM

```bash
# Find the VM's IP (if you don't know it yet)
ip -4 addr show scope global

# SSH in
ssh user@<VM_IP>
```

## 2. Update the OS and install essentials

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git curl \
  software-properties-common build-essential libssl-dev
```

## 3. Install Python 3.11 (recommended)

Airflow 2.10.2 is best tested with Python 3.11. Use it even if the VM ships a different default version:

```bash
sudo apt install -y software-properties-common
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3.11-dev
```

## 4. Create the project directory and clone this repo

```bash
mkdir -p ~/de_project
cd ~/de_project
git clone https://github.com/johnnyjohn993/airflwo-using-vm.git
```

## 5. Create and activate an isolated virtual environment

```bash
python3.11 -m venv ~/de_project/airflow_venv
source ~/de_project/airflow_venv/bin/activate
```

## 6. Set the Airflow home directory (persisted across sessions)

```bash
export AIRFLOW_HOME=~/airflow
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
```

## 7. Install Apache Airflow (using the official constraints)

Constraints pin Airflow's dependencies to versions known to work together, preventing dependency conflicts:

```bash
pip install --upgrade pip

AIRFLOW_VERSION=2.10.2
PYTHON_VERSION="$(python3.11 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

## 8. Install this repo's requirements

Installs the extra providers and libraries used by the project (pandas, boto3, Amazon and MS SQL providers) under the same constraints:

```bash
pip install -r ~/de_project/airflwo-using-vm/requirements.txt --constraint "${CONSTRAINT_URL}"
```

## 9. Disable the example DAGs

`airflow.cfg` only exists after Airflow's first run, so initialize the database first, then edit the config:

```bash
airflow db migrate

sed -i 's/load_examples = True/load_examples = False/' ~/airflow/airflow.cfg
grep "load_examples" ~/airflow/airflow.cfg

# Start with a clean metadata database
airflow db reset -y
```

## 10. Start Airflow

```bash
airflow standalone
```

`standalone` launches the webserver, scheduler, and triggerer with a single command.

- UI: `http://<VM_IP>:8080` (open `8080/tcp` in your cloud provider's firewall/security group, or `sudo ufw allow 8080/tcp` if ufw is enabled)
- Username: `admin`
- password: printed on startup, and saved in `~/airflow/standalone_admin_password.txt`

To stop: `Ctrl+C`. Next time, re-activate the venv first:

```bash
source ~/de_project/airflow_venv/bin/activate
airflow standalone
```

## 11. Load your DAGs

The sample DAG lives in this repo under `airflow/dag/`, but Airflow's default DAG folder is `~/airflow/dags`. Point Airflow at the repo folder:

```bash
sed -i "s|^dags_folder = .*|dags_folder = $HOME/de_project/airflwo-using-vm/airflow/dag|" ~/airflow/airflow.cfg
```

Then restart Airflow. Verify with:

```bash
airflow dags list
```

---

## Repo layout

| File / folder                          | Description                                                          |
| -------------------------------------- | -------------------------------------------------------------------- |
| `requirements.txt`                     | Pinned Airflow version + project dependencies (pandas, boto3, MSSQL) |
| `airflow/dag/sample.py`                | Sample `extract → transform → load` DAG (`basic_sample_dag`)         |
| `vm-commands.txt`                      | Raw command notes, including a full purge/clean-up sequence          |
| `gitclone.txt`                         | Notes for cloning the repo onto the VM                               |

## Uninstall / clean up

```bash
pkill -f airflow
deactivate 2>/dev/null || true

rm -rf ~/de_project ~/airflow
sed -i '/AIRFLOW_HOME/d' ~/.bashrc
unset AIRFLOW_HOME

# Optional: remove the system packages installed above
sudo apt purge -y python3-pip python3-venv build-essential libssl-dev
sudo apt autoremove -y
```
