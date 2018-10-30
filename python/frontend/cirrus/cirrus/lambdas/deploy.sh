#!/usr/bin/env bash

# ./deploy.sh <lambda_name>

if [ $# -eq 0 ]
  then
    echo "No function name supplied"
    exit -1
fi

FUNCTION_NAME=$1
DEPENDENCIES="handler.py mmh3* min_max_helper.py lambda_utils.py ../utils.py normal_helper.py redis/ rediscluster/ toml/ feature_hashing_helper.py"

rm bundle.zip -f
zip -9r bundle.zip $DEPENDENCIES
aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://bundle.zip&
aws logs delete-log-group --log-group-name /aws/lambda/$FUNCTION_NAME&
wait
rm bundle.zip -f
