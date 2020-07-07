.DEFAULT_GOAL := all
.PHONY : all install run

all: install run

install:
	python3 -m venv env
	env/bin/pip install -r requirements.txt

run:
	env/bin/python main.py daemon
