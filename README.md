# external-version-explanation

**Project:** "Version Management in Data Lakes" assignment - Advanced Topics in Computer Science 24/25 <br>
**Author:** Gianluca Farinaccio <br>
**Author:** Daniele Ferneti <br>

This report describes the solution designed and implemented to ad-
dress the ”Version Management in Data Lakes” assignment. We choose
the first point of assignment so the objective was to build a system that,
given two dataset versions, uses table search to find a set of candidate
tables that can explain the addition using a join. Since no data set
was provided, We built a small data set based on IMdb. This report
presents a complete solution for explaining external changes in data lakes
by identifying candidate tables that can account for attribute additions
via join operations.

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
