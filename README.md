# MockSparkContext

DISCLAIMER: this is not meant to be a replacement of testing on an actual spark context such as in https://github.com/holdenk/spark-testing-base
It is still useful to do this, but I personally prefer to have these as 'integration test' and not 'unit tests'
I just prefer to have unit tests which have the least amount of dependencies to outside systems (IO, databases, heavy frameworks...)
This is to increase the isolation of code and to speed up the unit tests