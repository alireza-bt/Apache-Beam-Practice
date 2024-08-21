# Apache-Beam-Practice
Practicing Apache Beam (DataFlow Model Batch/Streaming)

I followed the following quickstart guide to create a simple starter pipeline using local runner:

https://beam.apache.org/get-started/quickstart/python/#set-up-your-development-environment

It contains the followings:

- Installing python
- Cloning the beam-starter-python project from github
- Creating a venv
- Installing requirements
- Running the sample (main.py) in Python -> It's just a hello World! example to make sure if beam works correctly on local machine

## First Example: Wordcount
Source: [Beam Examples - wordcount.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py)

- Using direct-runner: (we can run the file locally or run it from beam modules)
```bash
python ./examples/wordcount/wordcount.py --input ./examples/wordcount/data/wikipedia-automobile.txt --output ./examples/wordcount/data/
OR
python -m apache_beam.examples.wordcount --input ./examples/wordcount/data/wikipedia-automobile.txt --output ./examples/wordcount/data/
```