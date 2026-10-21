VENV := .venv
BIN := $(VENV)/bin
BUILD := _build
SPHINXOPTS ?= -W --keep-going -n

.PHONY: install html run linkcheck spelling check clean

$(BIN)/sphinx-build: requirements.txt
	python3 -m venv $(VENV)
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet -r requirements.txt
	touch $@

install: $(BIN)/sphinx-build

html: install
	$(BIN)/sphinx-build -b dirhtml $(SPHINXOPTS) . $(BUILD)/html

run: install
	$(BIN)/sphinx-autobuild -b dirhtml . $(BUILD)/html --ignore $(VENV) --port 8000

linkcheck: install
	$(BIN)/sphinx-build -b linkcheck $(SPHINXOPTS) . $(BUILD)/linkcheck

spelling: install
	$(BIN)/codespell --skip "./$(VENV),./$(BUILD),*.svg" --ignore-words .wordlist.txt .

check: html spelling

clean:
	rm -rf $(BUILD)
