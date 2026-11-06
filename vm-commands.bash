# 1 update os packege repository and isntall essetials 
sudo apt update && sudo apt upgrade -y
suod apt install -y python3 python3-pip python3-venv git curl build-essentials libssl-dev

#2 create a project root directory
mkdir -p ~/de_project
cd ~/de_project

# 3 create an active and isoloated python env
python3 -m venv airflow_venv
source airflow_venv/bin/activate

#4. set Airflow home directory and presist it in ~/.bashrc
export AIRFLOW_HOME=~/airflowadde
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc

#5. install Apache Airflow using official constrains (prevents dependency conflicts)
AIRFLOW_VERSION=2.10.2
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PTYHON_VERSION}.txt"

pip install --upgrade pip
pip install "apache-airlfow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}
