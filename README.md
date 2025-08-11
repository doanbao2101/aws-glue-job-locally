# ⚙️ Run AWS Glue Jobs Locally with Docker + WSL

This guide helps you **run AWS Glue jobs locally** using **Docker** on **WSL (Windows Subsystem for Linux)**. It's useful for developing, testing, and debugging ETL scripts without deploying to AWS every time.

---

## ✅ Why Run AWS Glue Jobs Locally?

Running Glue jobs locally gives you several advantages:

- 🧪 **Faster Development Loop**: No need to wait for job deployment on the cloud.
- 💰 **Cost Savings**: Avoids AWS Glue compute charges during development.
- 🔧 **Flexible Debugging**: Easily test and debug locally using familiar tools.
- 🚫 **No Internet Dependency**: Work offline or behind firewalls.
- 🔍 **Detailed Logging**: Immediate access to local logs for quicker troubleshooting.

---

## 🏆 Prerequisites

Make sure you have the following:

- ✅ WSL2 with Ubuntu installed
- ✅ Docker Desktop installed & running
- ✅ AWS CLI installed and configured with SSO access
- ✅ Permissions to run Glue jobs
- ✅ Glue job scripts

---

## 🔧 Setup Instructions

### 1. Install WSL and Launch Ubuntu

```bash
wsl --install
wsl -d Ubuntu
```

### 2. Pull AWS Glue Docker Image

```bash
docker pull public.ecr.aws/glue/aws-glue-libs:5
```

> 💡 Use quotes if the path contains spaces.

### 3. Authenticate via AWS SSO

```bash
aws configure sso
aws sso login --profile replace_your_profile_name
```
---

## 📝 Notes

* 🐳 Ensure Docker is running under WSL integration.
* 🛠 Use logs printed by the container for debugging and iteration.

---

## 📚 References

* [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
* [Glue Docker Image – AWS Public ECR](https://gallery.ecr.aws/glue/aws-glue-libs)
* [WSL Installation Guide – Microsoft](https://learn.microsoft.com/en-us/windows/wsl/)

---
