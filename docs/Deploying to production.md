# Deploying to production

**Author:** [@lukazontar](https://github.com/lukazontar)

**Description:** This document outlines the steps to deploy the Luigi pipeline to production VM.

## Initialization of production VM

1. Generate a new SSH key pair:
    ```bash
    ssh-keygen -t ed25519 -f ssh-eucollab -C "Connection to eucollab VM."
    ```
2. Copy the public key to the VM.

3. **Connect to the VM**:
    - Open a terminal and connect to the VM using the following command:
    ```bash
    ssh -i ~/.ssh/ssh-eucollab eucollab@193.2.72.43
    ```
   


## MongoDB Setup
docker run -v .:/dump mongo mongorestore --gzip --uri "mongodb://root:rootpassword@mongo" mongodump.dump.gzip