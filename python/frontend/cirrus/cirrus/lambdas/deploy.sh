rm bundle.zip
zip -9r bundle.zip handler.py min_max_helper.py ../utils.py normal_helper.py redis/ rediscluster/ toml.py redis.toml feature_hashing_helper.py
aws lambda update-function-code --function-name neel_lambda --zip-file fileb://bundle.zip&
aws logs delete-log-group --log-group-name /aws/lambda/neel_lambda
rm bundle.zip
