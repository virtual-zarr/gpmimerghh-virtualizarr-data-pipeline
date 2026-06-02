## GPM IMERG Half-Hourly Virtualizarr Data Pipeline (shorthand: `gpmimerghh-vdp`)

This repo uses the Virtualizarr Data Pipelines (VDP) [github template
repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template) intended to help users create and manage Virtualizarr/Icechunk stores on AWS in a consistent, scalable way.

Read the [virtualizarr-data-pipelines (VDP)](https://github.com/developmentseed/virtualizarr-data-pipelines) documentation to learn more about the cloud architecture. Then head to the [design doc](design.md) to learn about how VDP was used for this dataset.

## Local development

### Set up the development environment

```sh
./scripts/setup.sh
```

#### Run unit tests

```sh
uv run pytest
```

Unit tests run against local fixtures and require no AWS access.

#### Run integration tests

Integration tests hit real S3 buckets and require valid AWS credentials and the
`ICECHUNK_BUCKET` / `ICECHUNK_PREFIX` environment variables to be set:

```
# set AWS credentials in your environment
uv run pytest -m integration
```

## Cloud Deployment

### NASA Earthdata credentials

GPM IMERG files require NASA Earthdata authentication. Store the credentials in AWS Secrets Manager rather than as plain text in `.env`:

**Create the secret** (once, before deploying):

```bash
aws secretsmanager create-secret \
  --name "<your-stack-name>/earthdata-credentials" \
  --description "NASA Earthdata credentials for the virtualizarr pipeline" \
  --secret-string '{"username":"<your-username>","password":"<your-password>"}' \
  --region <your-region>
```

Then set `EARTHDATA_SECRET_ARN` in your `.env` file to the ARN returned by the command above:

```
EARTHDATA_SECRET_ARN=arn:aws:secretsmanager:<region>:<account-id>:secret:<your-stack-name>/earthdata-credentials-<suffix>
```

The Lambda functions fetch the secret and set the credentials as environment variables for the Earthdata S3 credential provider.

### Create the .env file for other custom settings

```bash
cp .env.example .env
```

Modify `.env` as needed to customize the settings made available in `cdk/settings.py`.

#### Icechunk bucket settings

There are two bucket-related settings that control where the Icechunk store is written:

- **`ICECHUNK_BUCKET_NAME`** — the name of a new S3 bucket CDK will create for the store (default: `icechunk-outuput`).
- **`ICECHUNK_BUCKET`** — the name of an **existing** S3 bucket to use instead. When this is set, CDK will reference the bucket rather than create it.

If you already have a bucket (e.g. `nasa-eodc-public`), set `ICECHUNK_BUCKET=nasa-eodc-public` in your `.env` file to avoid the `already exists` error on deploy.

You can also set `ICECHUNK_PREFIX` for any additional path to the icechunk store.

#### Review your infrastructure before deploying

```bash
uv run --env-file .env.sample cdk synth
```

#### Deploy the CDK infrastructure.

```bash
uv run --env-file .env.sample cdk deploy
```
