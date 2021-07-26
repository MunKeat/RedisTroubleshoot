[![Python Linter](https://github.com/MunKeat/RedisTroubleshoot/actions/workflows/ci-python-linter.yml/badge.svg?branch=master)](https://github.com/MunKeat/RedisTroubleshoot/actions/workflows/ci-python-linter.yml) [![Unit Test](https://github.com/MunKeat/RedisTroubleshoot/actions/workflows/ci-unittesting.yml/badge.svg?branch=master)](https://github.com/MunKeat/RedisTroubleshoot/actions/workflows/ci-unittesting.yml)

## RedisTroubleshooter
RedisTroubleshooter is a collection of Python scripts that is used to troubleshoot some of the more common Redis problem.

### Testing
Testing is done against Dockerised Redis instance. 

To launch the dockerised Redis instance(s), run `make dev`.

To launch the test, run `python -m unittest discover test/`
