#!/bin/bash
set -e

sudo apt-get update
sudo apt-get install -y curl apt-transport-https docker.io conntrack

sudo usermod -aG docker $USER

curl -LO "https://dl.k8s.io/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x minikube
sudo mv minikube /usr/local/bin/

minikube start --driver=docker --cpus=2 --memory=4096
