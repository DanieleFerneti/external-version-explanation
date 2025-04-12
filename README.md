# external-version-explanation

**Project:** "Version Management in Data Lakes" assignment - Advanced Topics in Computer Science 24/25 <br>
**Author:** Gianluca Farinaccio <br>
**Author:** Daniele Ferneti <br>

In modern data lakes, datasets often evolve over time and external changes—such
as the addition of new attributes—must be explained. The aim of this assign-
ment is to build a system capable of finding candidate tables that, when joined
with a base (source) table, explain the observed attribute additions in a new
dataset version. In our approach, the system first generates synthetic candidate
tables from the base dataset, then performs various join operations, and finally
uses both schema and data content similarity to select the best candidate table.

## Project Structure

 >\src: source files to execute dataset creation and version explanation<br>
 >\dataset: base tables and joined tables<br>
 >\results: all results of your execution<br>

## Installation

* clone repo
```bash 
git clone https://github.com/gianlucafarinaccio/external-version-explanation.git 
```

* create your virtualenv
```bash 
python3 -m venv <env-name>
```
* activate virtualenv
```bash 
source <env-name>/bin/activate
```

* install dependencies via pip
```bash 
pip install -r requirements.txt
```


## Execute

* execute dataset creation 
```bash 
python3 src/make_dataset.py 
```

* execute joins between base tables 
```bash 
python3 src/make_join.py 
```

* execute main script 
```bash 
python3 src/external-version-explanation.py
```
