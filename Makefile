ENV_DIR := .venv

env:
	@echo "Creating virtual environment"
	python -m venv .venv

activate:
	powershell -noexit -executionpolicy bypass .venv/Scripts/activate.ps1

pip:
	@echo "Installing pip, setuptools, wheel"
	python -m pip install --upgrade pip setuptools wheel

prefect:
	@echo "Installing prefect 2"
	pip install -U prefect

install:
	@echo "Installing necessary packages"
	pip install python-dotenv requests google-cloud-storage

all: env activate pip prefect install

