rm bundle.zip
zip -9r bundle.zip handler.py MinMaxHandler.py ../utils.py NormalHandler.py redis/ rediscluster/ toml.py redis.toml FeatureHashingHandler.py
aws lambda update-function-code --function-name neel_lambda --zip-file fileb://bundle.zip&
aws logs delete-log-group --log-group-name /aws/lambda/neel_lambda
rm bundle.zip
