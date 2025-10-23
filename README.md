## Setup
1. Install prerequisites
- uv: https://docs.astral.sh/uv/getting-started/installation/

2. Run the dev setup script
```bash
./scripts/setup_dev/all.sh
```

3. _Optional_: Create a .env file in the root directory. Add environment variables to shell using:
```bash
export $(cat .env | xargs)
```
