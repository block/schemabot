# AWS Deployment

Deploy SchemaBot to AWS App Runner with GitHub App integration. This guide walks through the full setup from a fresh AWS account to testing `schemabot plan` on a PR.

The easiest way to get started is to clone or fork this repo so you have all the deployment scripts, Terraform configs, and example schema files ready to go.

## What Gets Created

- **App Runner Service** — SchemaBot API + webhook endpoint
- **RDS MySQL x3** — Storage + testapp staging/production
- **Bastion Instance** — For RDS access via SSM (~$3/month)
- **ECR Repository** — Docker image storage
- **Secrets Manager** — Database credentials + GitHub App credentials
- **VPC Endpoint** — For Secrets Manager access

**Approximate cost: ~$35-55/month** when running, $0 when stopped. These estimates may be out of date — check [AWS pricing](https://aws.amazon.com/pricing/) for current rates.

| Resource | Est. Cost | Notes |
|----------|-----------|-------|
| RDS db.t4g.micro x3 | ~$12/month each | SchemaBot storage + testapp staging/production |
| App Runner 0.25 vCPU | ~$3-5/month | Memory charged always, CPU only during requests |
| Bastion t4g.nano | ~$3/month | For database access via SSM |
| Secrets Manager | ~$1/month | 4 secrets |
| VPC Endpoint | ~$7/month | For Secrets Manager access from VPC |

This example creates dedicated staging and production databases — if you already have MySQL databases you want to manage, you can skip these and point SchemaBot at your existing databases instead (just update `config.yaml` with their DSNs). SchemaBot works with any MySQL 8.0+ instance. All resources can be stopped when not in use (see [Cost Control](#cost-control)).

## Prerequisites

- AWS account with admin access
- macOS with [Homebrew](https://brew.sh)

Install all dependencies:

```bash
brew install awscli terraform session-manager-plugin jq
brew install --cask docker
```

Verify installations:

```bash
aws --version          # AWS CLI v2
terraform --version    # Terraform >= 1.0
docker --version       # Docker
```

Open Docker Desktop at least once after installing to finish setup. The Session Manager plugin is needed later for connecting to RDS via the bastion.

## Step 1: Configure AWS CLI

### Create an IAM user

First step is to create an IAM user with the permissions needed to deploy infrastructure, and to create an AWS profile that can AWS CLI can use.

1. Sign in to the [AWS Console](https://console.aws.amazon.com/) as the root user
2. Go to **IAM > Users > Create user**
3. Enter a username (e.g. `schemabot-deployer`)
4. Click **Next**, then **Attach policies directly**
5. Search for and select **AdministratorAccess** (for simplicity — you can scope this down later)
6. Click **Create user**

### Create access keys

1. Click into the user you just created
2. Go to the **Security credentials** tab
3. Under **Access keys**, click **Create access key**
4. Select **Command Line Interface (CLI)**
5. Check the confirmation box, click **Next**, then **Create access key**
6. Copy the **Access key ID** and **Secret access key** (you won't see the secret again)

### Configure the CLI

Create a named profile matching the IAM user:

```bash
aws configure --profile schemabot-deployer
# AWS Access Key ID: <paste access key>
# AWS Secret Access Key: <paste secret key>
# Default region name: us-west-2
# Default output format: json
```

Verify:

```bash
aws sts get-caller-identity --profile schemabot-deployer
```

You should see your account ID and the `schemabot-deployer` user ARN.

## Step 2: Deploy Infrastructure

SchemaBot needs a few things to run:
- **Its own MySQL database** — stores plans, applies, locks, and other state
- **Target MySQL databases** — the application databases whose schemas you want to manage (this example creates staging + production instances for a `testapp` database)
- **A compute service** — runs the SchemaBot API server that handles CLI requests and GitHub webhooks

The bootstrap script creates all of this in your AWS account using Terraform (three RDS MySQL instances, an App Runner service, ECR, Secrets Manager, and a bastion), then builds and deploys the Docker image.

```bash
cd deploy/aws
./scripts/bootstrap.sh
```

The scripts default to the `schemabot-deployer` AWS profile. Override with `AWS_PROFILE` if you used a different name.

Terraform will show you everything it plans to create. Type `yes` to proceed. The full bootstrap takes ~15 minutes (mostly waiting for RDS instances to come up and the first Docker deploy).

When done, verify the service is running:

```bash
curl $(terraform output -raw service_url)/health
```

## Step 3: Create a GitHub App

Get the webhook URL:

```bash
terraform output -raw webhook_url && echo
```

Go to **GitHub > Settings > Developer settings > GitHub Apps > New GitHub App** ([direct link](https://github.com/settings/apps/new)).

Fill in:

| Setting | Value |
|---------|-------|
| **App name** | Something unique, e.g. `schemabot-pizza-palace` |
| **Homepage URL** | any repo url - `https://github.com/block/schemabot` |
| **Webhook URL** | The webhook URL from above |
| **Webhook secret** | Generate one: `openssl rand -hex 32` (save it) |

**Repository permissions:**

| Permission | Access |
|-----------|--------|
| Checks | Read & Write |
| Contents | Read & Write |
| Issues | Read & Write |
| Metadata | Read |
| Pull requests | Read & Write |

**Subscribe to events:**

- Create
- Issue comment
- Issues
- Pull request
- Pull request review
- Pull request review comment
- Push

**Where can this GitHub App be installed?**

- **Only on this account** — if you'll only use it on repos owned by this account
- **Any account** — if you need to install it on an organization or multiple accounts

Click **Create GitHub App**.

On the next page, note the **App ID** (you'll need it in the next step).

Scroll down to **Private keys** and click **Generate a private key** — this downloads a `.pem` file.

### Set the App Avatar

To give your bot a custom avatar, go to your GitHub App settings page and upload a logo. The [`assets/`](../../assets/) folder has SchemaBot logos in various sizes — `schemabot-avatar-1000.png` works well as a GitHub App logo.

## Step 4: Store GitHub App Credentials

```bash
./scripts/setup-github-app.sh --deploy
```

The script prompts for your App ID, private key file path, and webhook secret, stores them in Secrets Manager, and deploys the service. Run it again anytime to rotate credentials (omit `--deploy` if you don't need to redeploy).

## Step 5: Install the App

Go to [github.com/settings/apps](https://github.com/settings/apps), click your app name, then click **Install App** in the left sidebar. Choose the account or organization to install it on. You can grant access to all repositories or select specific ones.

## Step 6: Set Up Your Repository

The deployment already configured a `testapp` database with staging and production environments on the server side (see `deploy/aws/config.yaml`). The target databases start empty — SchemaBot will generate the DDL to create your tables when you run your first `plan`.

Now you need to set up the client side — your application repo needs a `schemabot.yaml` config file alongside the schema SQL files:

```
my-app/
  schema/
    schemabot.yaml
    users.sql
    orders.sql
```

`schemabot.yaml`:

```yaml
database: testapp
type: mysql
environments:
  - staging
  - production
```

The `database` field must match a database name in the SchemaBot server config. This example uses `testapp` which already matches the server config from bootstrap ([`config.yaml`](config.yaml)).

Each `.sql` file should contain a single `CREATE TABLE` using the canonical `SHOW CREATE TABLE` format:

```sql
CREATE TABLE `users` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `email` varchar(255) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

## Step 7: Test It

Open a PR that adds or modifies a `.sql` file, then comment:

```
schemabot plan -e staging
```

SchemaBot will:
1. React with :eyes: to acknowledge the command
2. Fetch the `schemabot.yaml` config and schema files from the PR branch
3. Diff the desired schema against the live staging database
4. Post a PR comment with the DDL plan
5. Create a GitHub Check Run showing the result

Other commands: `schemabot help`, `schemabot plan` (all environments), `schemabot plan -d mydb` (specific database).

## Using the CLI

The CLI can point at any SchemaBot server — local, AWS, or anywhere else:

```bash
# From the repo root
make install

# Configure your default endpoint (saves to ~/.config/schemabot/config.yaml)
schemabot configure
# SchemaBot endpoint [http://localhost:13370]: https://your-service.us-west-2.awsapprunner.com

# Now all commands use that endpoint automatically
schemabot plan -s examples/mysql/schema/testapp -e staging
schemabot apply -s examples/mysql/schema/testapp -e staging -y
```

You can also pass the endpoint directly without configuring:

```bash
schemabot plan -s examples/mysql/schema/testapp -e staging --endpoint https://your-service.us-west-2.awsapprunner.com
```

Or use named profiles to switch between multiple servers:

```bash
schemabot configure --profile aws     # AWS deployment
schemabot configure --profile local   # Local dev server

schemabot plan -s examples/mysql/schema/testapp -e staging --profile aws
```

## Helper Scripts

### Connect to Databases

```bash
./scripts/shell.sh                # testapp staging (default)
./scripts/shell.sh -e production  # testapp production
./scripts/shell.sh -e schemabot   # SchemaBot internal storage
```

### View Logs

```bash
./scripts/logs.sh            # Last 2 minutes
./scripts/logs.sh 10m        # Last 10 minutes
./scripts/logs.sh 5m error   # Filter for "error"
./scripts/logs.sh -f         # Follow in real-time
```

### Redeploy

```bash
./scripts/deploy.sh              # Full build + deploy
./scripts/deploy.sh --skip-build # Just trigger deployment
```

## Configuration

Secrets are stored in AWS Secrets Manager and resolved at runtime:

| Secret | Purpose |
|--------|---------|
| `schemabot-example/storage-dsn` | SchemaBot's internal database |
| `schemabot-example/testapp-staging` | Testapp staging database |
| `schemabot-example/testapp-production` | Testapp production database |
| `schemabot-example/github-app` | GitHub App private key + webhook secret |

The server config (`/config/aws-example.yaml` in the container) references these using the `secretsmanager:` prefix:

```yaml
storage:
  dsn: "secretsmanager:schemabot-example/storage-dsn#dsn"

github:
  app_id: "secretsmanager:schemabot-example/github-app#app_id"
  private_key: "secretsmanager:schemabot-example/github-app#private_key"
  webhook_secret: "secretsmanager:schemabot-example/github-app#webhook_secret"

databases:
  testapp:
    type: mysql
    environments:
      staging:
        dsn: "secretsmanager:schemabot-example/testapp-staging#dsn"
      production:
        dsn: "secretsmanager:schemabot-example/testapp-production#dsn"
```

Secret values support multiple backends: `env:VAR`, `file:/path`, `secretsmanager:name#key`, or plain text. See [pkg/secrets](../../pkg/secrets/) for details.

## Troubleshooting

**Health check fails:** Check App Runner logs with `./scripts/logs.sh 5m`.

**Webhook not receiving events:** Check **Recent Deliveries** on your GitHub App settings page.

**401 Unauthorized on webhook:** Webhook secret mismatch. Re-run `./scripts/setup-github-app.sh --deploy` to update the secret and redeploy.

**"No schemabot.yaml config found":** The `schemabot.yaml` file is missing or not in the schema directory alongside your SQL files.

**"database not found":** The `database` field in `schemabot.yaml` doesn't match any database in the server config.

## Cost Control

Stop resources when not in use:

```bash
# Stop bastion
aws ec2 stop-instances --instance-ids $(terraform output -raw bastion_instance_id)

# Stop RDS instances (takes a few minutes)
aws rds stop-db-instance --db-instance-identifier schemabot-example-storage
aws rds stop-db-instance --db-instance-identifier schemabot-example-testapp-staging
aws rds stop-db-instance --db-instance-identifier schemabot-example-testapp-production
```

## Cleanup

To tear down all infrastructure:

```bash
./scripts/destroy.sh
```

This always requires interactive confirmation — you must type `destroy` to proceed.
