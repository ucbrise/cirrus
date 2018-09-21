rm bundle.zip
zip -9 bundle.zip handler.py MinMaxHandler.py serialization.py
aws lambda update-function-code --function-name neel_lambda --zip-file fileb://bundle.zip&
aws logs delete-log-group --log-group-name /aws/lambda/neel_lambda
