Setup script flow
==================================

1. Setup AWS credentials (if not already set)
2. Setup preferred region
3. Create AMIa
4. Create IAM role
5. Setup S3 bucket
6. Configure lambda concurrency limit
7. Make server image (creates role, key pair in ~/.ssh/cirrus_key_pair.pem, security group and instance profile)
8. Save config to ~/.cirrus.cfg
