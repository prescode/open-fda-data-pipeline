![Logo of the project](./docs/logo.png)

# openFDA AWS data pipeline
> Enabling advanced analytics of openFDA data in AWS

This data pipeline leverages the power of AWS to:
* Automate the extraction data in bulk from the openFDA (really fast!)
* Loads the raw data into an S3 data lake
* Transforms the date with custom options for filtered fields and enrichment with NLP models to a curated (cleaned) S3 bucket
* Loads the data into Elasticsearch for advanced full text search and visualizations
* Enables other analytics to be executed on the data using AWS Glue, EMR, or other AWS analytics

An architecture overview:

![Architecture diagram](./docs/architecture_diagram.png)

## Getting started

The plan is to make the build automated using a CloudFormation or serverless template.  The current setup is a little manual.  See [building](#Building) for instructions.

### Initial Configuration

This project uses Python 3.8.  Testing the functions locally is possible using the AWS CLI.

## Developing

Clone the repository:

```shell
git clone https://github.com/prescode/open-fda-data-pipeline.git
```

Navigate to the function you want to change:

```shell
cd transform
```

Create a new virtual Python environment:

```shell
python3 -m venv .venv
```

Use pip to install the function's current dependencies into the virtual environment:

```shell
pip install -r ./requirements.txt -t
```
Add new dependencies to the requirements file (after installing them into your virtual environment using `pip install`)

```shell
pip freeze > requirements.txt
```

Activate your virtual environment:

```shell
source .venv/bin/activate
```

Start the Python shell:
```shell
python
```

Then test your code changes by pasting function definitions, variable assignments, and other code into the python shell.  Test events can be created using the json files to simulate S3 put events.

After making (and testing) your code changes close the python shell and deactivate the virtual environment:

```shell
deactivate
```

Remove your virtual environment (a build script will be used to create a new one for deployment):
```shell
rm -r .venv
```

### Building

Each function is built separately.  A `setup.sh` file is included in each folder.

```shell
./setup.sh
```
The shell script will create a virtual python environment, install all the necessary dependencies, then package the dependencies along with the function python file and create a `package.zip` file ready to be deployed to AWS Lambda.  The virtual environment and setup directory will be cleaned up after the process is complete.

### Deploying / Publishing

1. Create Lambda function via CLI or console
2. Upload `package.zip`
3. Update 

## Features

What's all the bells and whistles this project can perform?
* What's the main functionality
* You can also do another thing
* If you get really randy, you can even do this

## Configuration

Here you should write what are all of the configurations a user can enter when
using the project.

## Contributing

If you'd like to contribute, please fork the repository and use a feature
branch. Pull requests are warmly welcome.

## Links

- Repository: https://github.com/prescode/open-fda-data-pipeline/
- Issue tracker: https://github.com/prescode/open-fda-data-pipeline/issues
- openFDA website: https://open.fda.gov/
- AWS Elasticsearch: https://aws.amazon.com/elasticsearch-service/
- Related projects:
  - openFDA: https://github.com/FDA/openfda/

## Licensing

&copy; Dean Johnson - The code in this project is licensed under MIT license.
